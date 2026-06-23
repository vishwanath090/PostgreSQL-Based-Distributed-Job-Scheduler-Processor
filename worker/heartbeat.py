"""
worker/heartbeat.py
-------------------
heartbeat_loop(worker_id, pool)
    Every HEARTBEAT_INTERVAL seconds: touch heartbeat_at for all running jobs
    owned by this worker.  Proof-of-life for the stale-reaper.

stale_reaper_loop(pool)
    Every REAPER_INTERVAL seconds: reset jobs in claimed/running whose
    heartbeat_at is older than STALE_THRESHOLD back to pending.

Shutdown: both loops watch shutdown_event and exit after their current
iteration completes — they never terminate mid-UPDATE.

Advisory locks on worker crash:
    When a connection closes (normal or crash) Postgres automatically
    releases all session-scoped advisory locks.  The reaper does NOT call
    pg_advisory_unlock; it simply resets the status so another worker can
    acquire a fresh lock on the next claim.

Sleep pattern (no asyncio.shield task leak):
    We use asyncio.wait_for(event.wait(), timeout=N).  On timeout, wait_for
    cancels the event.wait() coroutine and raises TimeoutError — no side
    effects on the Event object.  The next iteration creates a fresh
    event.wait() coroutine, so there is no background task accumulation.
"""

import asyncio
import logging

import asyncpg

from worker.signals import shutdown_event

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 10   # seconds between writes
REAPER_INTERVAL    = 15   # seconds between reaper scans
STALE_THRESHOLD    = 30   # seconds without heartbeat → reclaim


async def _interruptible_sleep(seconds: float) -> bool:
    """
    Sleep for *seconds* or return early if shutdown_event is set.
    Returns True if shutdown was requested (caller should exit the loop).
    """
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=seconds)
        return True   # event was set — time to exit
    except asyncio.TimeoutError:
        return False  # normal timeout — keep going


async def heartbeat_loop(worker_id: str, pool: asyncpg.Pool) -> None:
    """Emit heartbeats for jobs running under *worker_id*."""
    logger.info("Heartbeat started for worker %s", worker_id)
    while not shutdown_event.is_set():
        try:
            result = await pool.execute(
                """
                UPDATE jobs
                SET    heartbeat_at = NOW()
                WHERE  worker_id = $1
                  AND  status    = 'running'
                """,
                worker_id,
            )
            n = int(result.split()[-1])
            if n:
                logger.debug("Heartbeat: updated %s row(s) for %s", n, worker_id)
        except asyncpg.PostgresError as exc:
            logger.warning("Heartbeat error (worker %s): %s", worker_id, exc)

        if await _interruptible_sleep(HEARTBEAT_INTERVAL):
            break

    logger.info("Heartbeat exiting for worker %s", worker_id)


async def stale_reaper_loop(pool: asyncpg.Pool) -> None:
    """
    Reclaim jobs from crashed or stalled workers.

    No explicit advisory-lock release needed: advisory locks are
    connection-scoped, so they are freed automatically when the dead
    worker's connection is dropped by Postgres.
    """
    logger.info("Stale-reaper started (threshold=%ss)", STALE_THRESHOLD)
    while not shutdown_event.is_set():
        try:
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
            n = int(result.split()[-1])
            if n:
                logger.warning("Stale reaper reclaimed %s job(s)", n)
        except asyncpg.PostgresError as exc:
            logger.error("Stale reaper error: %s", exc)

        if await _interruptible_sleep(REAPER_INTERVAL):
            break

    logger.info("Stale-reaper exiting")
