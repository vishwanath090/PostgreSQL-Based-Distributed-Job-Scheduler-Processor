"""
tests/test_priority.py
----------------------
Verifies that workers always claim the highest-priority eligible job first.

The partial index idx_jobs_queue on jobs(priority DESC, run_at ASC) WHERE
status='pending' combined with ORDER BY priority DESC, run_at ASC in the
claim query ensures deterministic priority ordering even under concurrency.

A single serial worker is used for ordering tests to make completion order
deterministic (two workers could claim different jobs simultaneously).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import pytest_asyncio

from worker.worker import run_worker, _try_claim
from worker.signals import shutdown_event

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def reset_shutdown():
    shutdown_event.clear()
    yield
    shutdown_event.clear()


# ---------------------------------------------------------------------------
# Integration: completion order matches priority DESC
# ---------------------------------------------------------------------------

async def test_priority_order_single_worker(
    pool: asyncpg.Pool,
    enqueue,
    test_dsn: str,
):
    """
    Jobs with priority 1, 5, 10 must complete in order 10 → 5 → 1.
    Verified via completed_at timestamps.
    """
    id_low  = await enqueue(type="noop", priority=1)
    id_mid  = await enqueue(type="noop", priority=5)
    id_high = await enqueue(type="noop", priority=10)

    # Confirm all pending before starting
    pending = await pool.fetch(
        "SELECT priority FROM jobs WHERE status='pending' ORDER BY priority DESC"
    )
    assert [r["priority"] for r in pending] == [10, 5, 1]

    workers = [asyncio.create_task(run_worker("prio-w0", pool, dsn=test_dsn))]

    # Wait for all 3 to finish
    for _ in range(50):
        done_count = await pool.fetchval(
            "SELECT COUNT(*) FROM jobs WHERE status='done'"
        )
        if done_count == 3:
            break
        await asyncio.sleep(0.1)

    shutdown_event.set()
    await asyncio.gather(*workers, return_exceptions=True)
    shutdown_event.clear()

    rows = await pool.fetch(
        """
        SELECT id, priority, completed_at
        FROM   jobs
        WHERE  id = ANY($1::bigint[])
        ORDER BY completed_at ASC NULLS LAST
        """,
        [id_low, id_mid, id_high],
    )

    assert len(rows) == 3
    assert all(r["completed_at"] is not None for r in rows)

    completion_order = [r["priority"] for r in rows]
    assert completion_order == [10, 5, 1], (
        f"Expected [10, 5, 1], got {completion_order}. "
        "Priority index or claim ORDER BY is incorrect."
    )


# ---------------------------------------------------------------------------
# Unit: _try_claim returns highest-priority job
# ---------------------------------------------------------------------------

async def test_try_claim_returns_highest_priority(
    pool: asyncpg.Pool,
    enqueue,
    test_dsn: str,
):
    """_try_claim must select the highest-priority pending job."""
    _low  = await enqueue(type="noop", priority=2)
    _mid  = await enqueue(type="noop", priority=6)
    id_hi = await enqueue(type="noop", priority=9)

    import asyncpg as _apg
    claim_conn = await _apg.connect(dsn=test_dsn)
    import json
    await claim_conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )

    try:
        claimed = await _try_claim(claim_conn, "prio-unit-w")
        if claimed:
            await claim_conn.execute("SELECT pg_advisory_unlock($1)", claimed["id"])
    finally:
        await claim_conn.close()

    assert claimed is not None
    assert claimed["id"] == id_hi, (
        f"Expected job with priority=9 (id={id_hi}), got id={claimed['id']} "
        f"priority={claimed['priority']}"
    )


# ---------------------------------------------------------------------------
# Tiebreak: same priority, earlier run_at wins
# ---------------------------------------------------------------------------

async def test_priority_tiebreak_by_run_at(
    pool: asyncpg.Pool,
    test_dsn: str,
):
    """When priorities are equal, the earlier run_at is claimed first."""
    now = datetime.now(timezone.utc)
    older_id = await pool.fetchval(
        "INSERT INTO jobs (type, priority, run_at) VALUES ('noop', 5, $1) RETURNING id",
        now - timedelta(seconds=10),
    )
    _newer_id = await pool.fetchval(
        "INSERT INTO jobs (type, priority, run_at) VALUES ('noop', 5, $1) RETURNING id",
        now,
    )

    import asyncpg as _apg, json
    claim_conn = await _apg.connect(dsn=test_dsn)
    await claim_conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    try:
        claimed = await _try_claim(claim_conn, "tiebreak-w")
        if claimed:
            await claim_conn.execute("SELECT pg_advisory_unlock($1)", claimed["id"])
    finally:
        await claim_conn.close()

    assert claimed["id"] == older_id, (
        f"Expected older job (id={older_id}) to be claimed first, got {claimed['id']}"
    )


# ---------------------------------------------------------------------------
# Scheduled jobs not claimed before run_at
# ---------------------------------------------------------------------------

async def test_future_scheduled_job_not_claimed(
    pool: asyncpg.Pool,
    test_dsn: str,
):
    """A job with run_at in the future must not be claimed."""
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await pool.execute(
        "INSERT INTO jobs (type, priority, run_at) VALUES ('noop', 10, $1)",
        future,
    )

    import asyncpg as _apg, json
    claim_conn = await _apg.connect(dsn=test_dsn)
    await claim_conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    try:
        claimed = await _try_claim(claim_conn, "sched-w")
        if claimed:
            await claim_conn.execute("SELECT pg_advisory_unlock($1)", claimed["id"])
    finally:
        await claim_conn.close()

    assert claimed is None, (
        "Worker claimed a future-scheduled job — AND run_at <= NOW() is missing."
    )
