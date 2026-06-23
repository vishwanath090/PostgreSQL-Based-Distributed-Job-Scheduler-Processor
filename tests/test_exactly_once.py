"""
tests/test_exactly_once.py
--------------------------
The most critical correctness test.

5 concurrent workers race to claim the same single job.  The job must be
executed exactly once — no double execution, no skipped execution.

Why this is hard without SKIP LOCKED + pg_try_advisory_lock
------------------------------------------------------------
Naive SELECT/UPDATE pattern:
  Worker A: SELECT id FROM jobs WHERE status='pending' → gets id=1
  Worker B: SELECT id FROM jobs WHERE status='pending' → also gets id=1
  Worker A: UPDATE … SET status='claimed' WHERE id=1   ← wins
  Worker B: UPDATE … SET status='claimed' WHERE id=1   ← also wins (race!)

SKIP LOCKED prevents the second SELECT seeing the row while Worker A's
transaction holds the row lock.  pg_try_advisory_lock adds session-scoped
protection so the job stays protected during execution — after the claim
transaction commits, while the handler is still running.

Test structure
--------------
1. Insert 1 job.
2. Spin 5 worker tasks (each uses its own claim_conn → separate backend sessions
   → separate advisory lock slots).
3. Wait 2 s for at least one worker to complete.
4. Cancel all workers via shutdown_event.
5. Assert status='done' and attempt=1 (executed exactly once).
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest
import pytest_asyncio

from worker.worker import run_worker
from worker.signals import shutdown_event

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def reset_shutdown():
    shutdown_event.clear()
    yield
    shutdown_event.clear()


# ---------------------------------------------------------------------------
# 5 workers, 1 job
# ---------------------------------------------------------------------------

async def test_exactly_once_single_job(
    pool: asyncpg.Pool,
    enqueue,
    test_dsn: str,
):
    """5 workers compete for 1 job — job must be done with attempt == 1."""
    job_id = await enqueue(type="noop", payload={})

    workers = [
        asyncio.create_task(run_worker(f"eo-w{i}", pool, dsn=test_dsn))
        for i in range(5)
    ]

    await asyncio.sleep(2)

    shutdown_event.set()
    await asyncio.gather(*workers, return_exceptions=True)
    shutdown_event.clear()

    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)

    assert row is not None
    assert row["status"] == "done", (
        f"Expected 'done', got '{row['status']}'. "
        "Job may not have been picked up or a worker crashed."
    )
    assert row["attempt"] == 1, (
        f"Expected attempt=1, got {row['attempt']}. "
        "Double-execution detected — advisory lock or SKIP LOCKED is broken."
    )
    assert row["result"] == {"ok": True}


# ---------------------------------------------------------------------------
# 3 workers, 10 jobs
# ---------------------------------------------------------------------------

async def test_exactly_once_many_jobs(
    pool: asyncpg.Pool,
    enqueue,
    test_dsn: str,
):
    """3 workers, 10 jobs — every job done exactly once."""
    job_ids = [await enqueue(type="noop") for _ in range(10)]

    workers = [
        asyncio.create_task(run_worker(f"eo-mw{i}", pool, dsn=test_dsn))
        for i in range(3)
    ]

    await asyncio.sleep(4)

    shutdown_event.set()
    await asyncio.gather(*workers, return_exceptions=True)
    shutdown_event.clear()

    rows = await pool.fetch(
        "SELECT id, status, attempt FROM jobs WHERE id = ANY($1::bigint[])",
        job_ids,
    )

    for row in rows:
        assert row["status"] == "done", (
            f"Job {row['id']}: expected 'done', got '{row['status']}'"
        )
        assert row["attempt"] == 1, (
            f"Job {row['id']}: expected attempt=1, got {row['attempt']} "
            "(possible double execution)"
        )

    assert len(rows) == 10


# ---------------------------------------------------------------------------
# Advisory lock semantics directly
# ---------------------------------------------------------------------------

async def test_advisory_lock_prevents_double_claim(
    pool: asyncpg.Pool,
    enqueue,
):
    """
    pg_try_advisory_lock returns FALSE when another session holds the lock.
    This directly validates the core of the exactly-once mechanism.
    """
    job_id = await enqueue(type="noop")

    async with pool.acquire() as conn1, pool.acquire() as conn2:
        locked_by_1 = await conn1.fetchval(
            "SELECT pg_try_advisory_lock($1)", job_id
        )
        assert locked_by_1 is True, "First lock acquisition should succeed"

        locked_by_2 = await conn2.fetchval(
            "SELECT pg_try_advisory_lock($1)", job_id
        )
        assert locked_by_2 is False, (
            "pg_try_advisory_lock must return FALSE when lock is already held. "
            "Double-claim protection is broken."
        )

        await conn1.execute("SELECT pg_advisory_unlock($1)", job_id)
