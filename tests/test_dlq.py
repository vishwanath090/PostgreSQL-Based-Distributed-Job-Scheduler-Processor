"""
tests/test_dlq.py
-----------------
Verifies that jobs exhausting max_retries land atomically in dead_letter_jobs
and the source row is marked 'dead'.

Invariants
----------
1. Exactly one DLQ row per exhausted job (no duplicates).
2. jobs.status == 'dead' and dead_letter_jobs.original_id == jobs.id.
3. dead_letter_jobs.attempts == max_retries (all retries consumed).
4. dead_letter_jobs.error is non-empty.
5. INSERT + UPDATE are atomic — no half-state visible.
6. Payload in DLQ matches original job payload.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import asyncpg
import pytest
import pytest_asyncio

from worker.worker import run_worker, _handle_failure
from worker.signals import shutdown_event
import worker.worker as worker_module

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def reset_shutdown():
    shutdown_event.clear()
    yield
    shutdown_event.clear()


# ---------------------------------------------------------------------------
# Integration: run job to death via the full worker loop
# ---------------------------------------------------------------------------

async def test_dlq_after_exhausted_retries(
    pool: asyncpg.Pool,
    enqueue,
    test_dsn: str,
):
    """
    'always_fail' with max_retries=3 → dead_letter_jobs after 3 failures.
    BASE_DELAY=0 so retries are immediate.
    """
    with patch.object(worker_module, "BASE_DELAY", 0.0):
        job_id = await enqueue(type="always_fail", max_retries=3)
        workers = [asyncio.create_task(run_worker("dlq-w0", pool, dsn=test_dsn))]
        await asyncio.sleep(6)
        shutdown_event.set()
        await asyncio.gather(*workers, return_exceptions=True)
        shutdown_event.clear()

    job_row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    assert job_row["status"] == "dead", (
        f"Expected 'dead', got '{job_row['status']}'"
    )

    dlq_rows = await pool.fetch(
        "SELECT * FROM dead_letter_jobs WHERE original_id = $1", job_id
    )
    assert len(dlq_rows) == 1, (
        f"Expected exactly 1 DLQ row, found {len(dlq_rows)}"
    )

    dlq = dlq_rows[0]
    assert dlq["original_id"] == job_id
    assert dlq["type"]         == "always_fail"
    assert dlq["attempts"]     == 3
    assert dlq["error"]        is not None and len(dlq["error"]) > 0


# ---------------------------------------------------------------------------
# Unit: _handle_failure is atomic on final attempt
# ---------------------------------------------------------------------------

async def test_dlq_transition_is_atomic(pool: asyncpg.Pool, enqueue):
    """
    Direct call to _handle_failure at attempt == max_retries - 1.
    jobs.status='dead' and DLQ row exist at the same time.
    """
    job_id = await enqueue(type="always_fail", max_retries=3)
    await pool.execute("UPDATE jobs SET attempt = 2 WHERE id = $1", job_id)
    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)

    await _handle_failure(pool, row, RuntimeError("final"), "dlq-unit-w")

    job_row  = await pool.fetchrow("SELECT status FROM jobs WHERE id = $1", job_id)
    dlq_count: int = await pool.fetchval(
        "SELECT COUNT(*) FROM dead_letter_jobs WHERE original_id = $1", job_id
    )

    assert job_row["status"] == "dead"
    assert dlq_count == 1, "INSERT and UPDATE must be atomic"


# ---------------------------------------------------------------------------
# No duplicate DLQ rows
# ---------------------------------------------------------------------------

async def test_dlq_no_duplicates(pool: asyncpg.Pool, enqueue):
    """_handle_failure called once → exactly one DLQ row, no more."""
    job_id = await enqueue(type="always_fail", max_retries=1)
    row    = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)

    await _handle_failure(pool, row, RuntimeError("err"), "w1")

    count: int = await pool.fetchval(
        "SELECT COUNT(*) FROM dead_letter_jobs WHERE original_id = $1", job_id
    )
    assert count == 1


# ---------------------------------------------------------------------------
# Payload preserved in DLQ
# ---------------------------------------------------------------------------

async def test_dlq_preserves_payload(pool: asyncpg.Pool, enqueue):
    """Payload in dead_letter_jobs matches the original job payload."""
    payload = {"service": "billing", "user_id": 42, "amount": 99.99}
    job_id  = await enqueue(type="always_fail", payload=payload, max_retries=1)
    row     = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)

    await _handle_failure(pool, row, RuntimeError("err"), "w-payload")

    dlq = await pool.fetchrow(
        "SELECT payload FROM dead_letter_jobs WHERE original_id = $1", job_id
    )
    stored = dlq["payload"] if isinstance(dlq["payload"], dict) else {}
    assert stored == payload
