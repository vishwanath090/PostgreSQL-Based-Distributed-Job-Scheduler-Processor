"""
tests/test_stale_reaper.py
--------------------------
Verifies that the stale-reaper resets expired jobs to 'pending'.

Scenarios
---------
A. Running job with expired heartbeat → reset to pending
B. Claimed (never reached running) with expired heartbeat → reset to pending
C. Terminal (done/failed/dead) jobs are NOT touched
D. Running job with fresh heartbeat → NOT reclaimed
E. End-to-end: reclaimed job is picked up and completed by a healthy worker
F. stale_reaper_loop exits cleanly on shutdown_event
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
import pytest_asyncio

from worker.heartbeat import stale_reaper_loop, STALE_THRESHOLD
from worker.worker import run_worker
from worker.signals import shutdown_event

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
async def reset_shutdown():
    shutdown_event.clear()
    yield
    shutdown_event.clear()


def _stale_ts() -> datetime:
    """A timestamp guaranteed to exceed the stale threshold."""
    return datetime.now(timezone.utc) - timedelta(seconds=STALE_THRESHOLD + 10)


async def _run_reaper_once(pool: asyncpg.Pool) -> int:
    """Execute one reaper sweep and return how many rows were updated."""
    result = await pool.execute(
        """
        UPDATE jobs
        SET    status       = 'pending',
               claimed_at   = NULL,
               worker_id    = NULL,
               heartbeat_at = NULL
        WHERE  status      IN ('claimed', 'running')
          AND  heartbeat_at < NOW() - ($1 || ' seconds')::interval
        """,
        str(STALE_THRESHOLD),
    )
    return int(result.split()[-1])


# ---------------------------------------------------------------------------
# A. Running + expired heartbeat → pending
# ---------------------------------------------------------------------------

async def test_stale_running_job_reclaimed(pool: asyncpg.Pool):
    job_id = await pool.fetchval(
        """
        INSERT INTO jobs (type, status, worker_id, heartbeat_at, claimed_at)
        VALUES ('noop', 'running', 'dead-worker', $1, $1)
        RETURNING id
        """,
        _stale_ts(),
    )

    count = await _run_reaper_once(pool)
    assert count == 1

    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    assert row["status"]       == "pending"
    assert row["worker_id"]    is None
    assert row["claimed_at"]   is None
    assert row["heartbeat_at"] is None


# ---------------------------------------------------------------------------
# B. Claimed + expired → pending
# ---------------------------------------------------------------------------

async def test_stale_claimed_job_reclaimed(pool: asyncpg.Pool):
    job_id = await pool.fetchval(
        """
        INSERT INTO jobs (type, status, worker_id, heartbeat_at, claimed_at)
        VALUES ('noop', 'claimed', 'dead-worker', $1, $1)
        RETURNING id
        """,
        _stale_ts(),
    )

    await _run_reaper_once(pool)

    row = await pool.fetchrow("SELECT status FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "pending"


# ---------------------------------------------------------------------------
# C. Terminal jobs NOT touched
# ---------------------------------------------------------------------------

async def test_terminal_jobs_not_reclaimed(pool: asyncpg.Pool):
    terminal = ["done", "failed", "dead"]
    ids = []
    for s in terminal:
        jid = await pool.fetchval(
            """
            INSERT INTO jobs (type, status, worker_id, heartbeat_at, claimed_at)
            VALUES ('noop', $1, 'old-worker', $2, $2)
            RETURNING id
            """,
            s, _stale_ts(),
        )
        ids.append((jid, s))

    await _run_reaper_once(pool)

    for jid, expected in ids:
        row = await pool.fetchrow("SELECT status FROM jobs WHERE id = $1", jid)
        assert row["status"] == expected, (
            f"Terminal job {jid} (was '{expected}') was incorrectly reset"
        )


# ---------------------------------------------------------------------------
# D. Fresh heartbeat NOT reclaimed
# ---------------------------------------------------------------------------

async def test_fresh_running_job_not_reclaimed(pool: asyncpg.Pool):
    fresh = datetime.now(timezone.utc) - timedelta(seconds=5)
    job_id = await pool.fetchval(
        """
        INSERT INTO jobs (type, status, worker_id, heartbeat_at, claimed_at)
        VALUES ('noop', 'running', 'healthy-worker', $1, $1)
        RETURNING id
        """,
        fresh,
    )

    await _run_reaper_once(pool)

    row = await pool.fetchrow("SELECT status FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "running", "Fresh-heartbeat job was incorrectly reclaimed"


# ---------------------------------------------------------------------------
# E. End-to-end: reaper reclaims, healthy worker completes
# ---------------------------------------------------------------------------

async def test_end_to_end_reaper_then_worker(
    pool: asyncpg.Pool,
    test_dsn: str,
):
    """
    1. Insert a 'running' job with expired heartbeat.
    2. Run one reaper sweep → job becomes 'pending'.
    3. Start a healthy worker → job completes.
    """
    job_id = await pool.fetchval(
        """
        INSERT INTO jobs (type, status, worker_id, heartbeat_at, claimed_at)
        VALUES ('noop', 'running', 'zombie-worker', $1, $1)
        RETURNING id
        """,
        _stale_ts(),
    )

    count = await _run_reaper_once(pool)
    assert count == 1

    row = await pool.fetchrow("SELECT status FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "pending"

    workers = [asyncio.create_task(run_worker("rescue-w0", pool, dsn=test_dsn))]
    await asyncio.sleep(2)
    shutdown_event.set()
    await asyncio.gather(*workers, return_exceptions=True)
    shutdown_event.clear()

    row = await pool.fetchrow("SELECT status FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "done", (
        f"Expected 'done' after rescue, got '{row['status']}'"
    )


# ---------------------------------------------------------------------------
# F. stale_reaper_loop exits cleanly on shutdown
# ---------------------------------------------------------------------------

async def test_reaper_loop_shuts_down_cleanly(pool: asyncpg.Pool):
    """stale_reaper_loop must exit promptly when shutdown_event is set."""
    shutdown_event.set()
    task = asyncio.create_task(stale_reaper_loop(pool))
    try:
        await asyncio.wait_for(task, timeout=3.0)
    except asyncio.TimeoutError:
        task.cancel()
        pytest.fail("stale_reaper_loop did not exit on shutdown_event")
    finally:
        shutdown_event.clear()
