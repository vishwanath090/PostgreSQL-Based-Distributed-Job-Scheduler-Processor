"""
tests/test_retry.py
-------------------
Verifies exponential back-off retry behaviour.

'flaky' handler: fails on attempt 0 and 1, succeeds on attempt 2.

Key assertions
--------------
  • After each failure: status='pending', attempt incremented, run_at future
  • After full cycle completes: status='done', result contains attempt index
  • Backoff formula: run_at ≈ NOW() + 2^attempt * BASE_DELAY seconds
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
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
# A: first failure sets run_at to the future
# ---------------------------------------------------------------------------

async def test_retry_sets_run_at_to_future(
    pool: asyncpg.Pool,
    enqueue,
    test_dsn: str,
):
    """After the first failure, run_at must be strictly in the future."""
    with patch.object(worker_module, "BASE_DELAY", 60.0):
        job_id = await enqueue(type="flaky", max_retries=3)
        workers = [asyncio.create_task(run_worker("retry-w0", pool, dsn=test_dsn))]
        await asyncio.sleep(1.5)
        shutdown_event.set()
        await asyncio.gather(*workers, return_exceptions=True)
        shutdown_event.clear()

    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "pending", (
        f"Expected 'pending' after first failure, got '{row['status']}'"
    )
    assert row["attempt"] == 1

    now_utc = datetime.now(timezone.utc)
    run_at  = row["run_at"].replace(tzinfo=timezone.utc)
    assert run_at > now_utc, (
        f"run_at={run_at} should be in the future — back-off not applied"
    )
    assert row["error"] is not None


# ---------------------------------------------------------------------------
# B: full retry cycle completes (BASE_DELAY=0 for speed)
# ---------------------------------------------------------------------------

async def test_full_retry_cycle_flaky_job(
    pool: asyncpg.Pool,
    enqueue,
    test_dsn: str,
):
    """
    flaky: fails on attempt 0 and 1, succeeds on attempt 2.
    With BASE_DELAY=0 the retries are immediate so we can observe the full
    lifecycle without sleeping for real time.

    Expected: status='done', result={'ok': True, 'succeeded_on_attempt': 2}
    """
    with patch.object(worker_module, "BASE_DELAY", 0.0):
        job_id = await enqueue(type="flaky", max_retries=3)
        workers = [asyncio.create_task(run_worker("retry-cycle-w0", pool, dsn=test_dsn))]
        await asyncio.sleep(5)
        shutdown_event.set()
        await asyncio.gather(*workers, return_exceptions=True)
        shutdown_event.clear()

    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    assert row["status"] == "done", (
        f"Expected 'done' after full retry cycle, got '{row['status']}'. "
        f"error={row['error']}"
    )
    assert row["result"] == {"ok": True, "succeeded_on_attempt": 2}


# ---------------------------------------------------------------------------
# C: attempt counter increments on each failure
# ---------------------------------------------------------------------------

async def test_attempt_increments_on_each_failure(pool: asyncpg.Pool, enqueue):
    """Call _handle_failure directly twice and verify attempt increments."""
    job_id = await enqueue(type="always_fail", max_retries=5)
    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)

    with patch.object(worker_module, "BASE_DELAY", 0.0):
        await _handle_failure(pool, row, RuntimeError("failure 1"), "test-worker")
    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    assert row["attempt"] == 1
    assert row["status"]  == "pending"

    with patch.object(worker_module, "BASE_DELAY", 0.0):
        await _handle_failure(pool, row, RuntimeError("failure 2"), "test-worker")
    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    assert row["attempt"] == 2
    assert row["status"]  == "pending"


# ---------------------------------------------------------------------------
# D: backoff formula run_at = NOW() + 2^attempt * BASE_DELAY
# ---------------------------------------------------------------------------

async def test_backoff_delay_formula(pool: asyncpg.Pool, enqueue):
    """
    Verify the exact backoff formula for attempt 0, 1, and 2 with BASE_DELAY=10:
      attempt 0 → 10 s
      attempt 1 → 20 s
      attempt 2 → 40 s
    """
    base = 10.0

    for attempt_num in range(3):
        job_id = await enqueue(type="always_fail", max_retries=5)
        await pool.execute(
            "UPDATE jobs SET attempt = $1 WHERE id = $2",
            attempt_num, job_id,
        )
        row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)

        before = datetime.now(timezone.utc)
        with patch.object(worker_module, "BASE_DELAY", base):
            await _handle_failure(pool, row, RuntimeError("err"), "test-w")
        after = datetime.now(timezone.utc)

        updated = await pool.fetchrow("SELECT run_at FROM jobs WHERE id = $1", job_id)
        expected_delay = (2 ** attempt_num) * base
        run_at = updated["run_at"].replace(tzinfo=timezone.utc)

        expected_min = before + timedelta(seconds=expected_delay - 1)
        expected_max = after  + timedelta(seconds=expected_delay + 3)

        assert expected_min <= run_at <= expected_max, (
            f"attempt={attempt_num}: expected run_at ≈ now+{expected_delay:.0f}s, "
            f"got {run_at} (window {expected_min}–{expected_max})"
        )
