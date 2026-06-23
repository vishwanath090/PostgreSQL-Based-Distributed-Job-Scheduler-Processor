"""
worker/worker.py
----------------
Async worker: LISTEN/NOTIFY wakeup, exactly-once claim, execute, retry/DLQ.

Exactly-once delivery — two complementary locks
------------------------------------------------
1. FOR UPDATE SKIP LOCKED  (row-level, transaction-scoped)
   When Worker A locks row 42 inside a transaction, Worker B's SELECT with
   SKIP LOCKED silently skips it rather than blocking. Eliminates the TOCTOU
   race at the moment of selection.

2. pg_try_advisory_lock(id)  (session-level, connection-scoped)
   FOR UPDATE drops its lock the moment the claim transaction COMMITs.
   Without an advisory lock, another worker could re-claim the same job
   between "commit" and "handler returns". pg_try_advisory_lock holds for
   the lifetime of the *connection* (not the transaction), so the job stays
   exclusively owned until pg_advisory_unlock is called explicitly — or the
   connection closes, which frees the lock automatically (no orphaned locks
   on crash).

   Both lock operations run on the same dedicated claim_conn. Calling
   pg_advisory_unlock on a different pool connection would silently target a
   different backend session and leave the lock held — a critical bug avoided
   by never pooling the claim connection.

LISTEN/NOTIFY wakeup
--------------------
Each worker opens one dedicated listen_conn. The DB trigger fires
pg_notify('job_channel', id) on every INSERT. The asyncpg callback posts
notify_event.set() into the asyncio event loop, so workers wake up in ~1 ms
instead of waiting for the 5-second polling fallback.
"""

import asyncio
import json
import logging
import os
import signal
import time
import uuid
from typing import Optional

import asyncpg

from db.pool import create_pool
from worker.signals import shutdown_event
from worker.registry import get_handler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (all overridable via environment variables)
# ---------------------------------------------------------------------------
BASE_DELAY    = float(os.environ.get("RETRY_BASE_DELAY", "5"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL",    "5"))
WORKER_COUNT  = int(os.environ.get("WORKER_COUNT",        "4"))


def _db_dsn() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://taskuser:taskpass@postgres:5432/taskqueue",
    )


# Per-worker asyncio.Event, signalled by the NOTIFY callback.
_notify_events: dict[str, asyncio.Event] = {}


# ---------------------------------------------------------------------------
# JSONB codec: register on each raw connection so asyncpg ↔ Python dicts
# ---------------------------------------------------------------------------

async def _init_conn(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


# ---------------------------------------------------------------------------
# LISTEN/NOTIFY callback
# ---------------------------------------------------------------------------

def _make_notify_callback(worker_id: str):
    """Return a closure that signals the worker's notify_event."""
    def _on_notify(_conn, _pid, _channel, payload):
        logger.debug("Worker %s notified: job %s", worker_id, payload)
        ev = _notify_events.get(worker_id)
        if ev is None:
            return
        # asyncpg calls this from its I/O thread; use call_soon_threadsafe
        # to safely post into the asyncio event loop.
        try:
            loop = asyncio.get_event_loop()
            loop.call_soon_threadsafe(ev.set)
        except RuntimeError:
            ev.set()
    return _on_notify


# ---------------------------------------------------------------------------
# Job claim — the critical section
# ---------------------------------------------------------------------------

async def _try_claim(
    conn: asyncpg.Connection,
    worker_id: str,
) -> Optional[asyncpg.Record]:
    """
    Attempt to claim exactly one pending, eligible job on *conn*.

    *conn* MUST be the worker's dedicated claim connection — not a short-lived
    pool connection — because pg_try_advisory_lock is session-scoped and must
    remain held on the same backend until pg_advisory_unlock is called after
    execution finishes (see module docstring).

    Returns the job record if claimed, None if nothing is available.
    """
    async with conn.transaction():
        row = await conn.fetchrow("""
            SELECT id, type, payload, status, priority, attempt,
                   max_retries, run_at, worker_id, error
            FROM   jobs
            WHERE  status  = 'pending'
              AND  run_at <= NOW()
              AND  pg_try_advisory_lock(id)
            ORDER BY priority DESC, run_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """)

        if row is None:
            return None

        await conn.execute("""
            UPDATE jobs
            SET    status     = 'claimed',
                   claimed_at = NOW(),
                   worker_id  = $1
            WHERE  id         = $2
        """, worker_id, row["id"])

    return row


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------

async def _execute_job(
    pool: asyncpg.Pool,
    claim_conn: asyncpg.Connection,
    job: asyncpg.Record,
    worker_id: str,
) -> None:
    """
    Run the handler for *job*, write result, release the advisory lock.

    The advisory lock was acquired on *claim_conn* inside _try_claim; it MUST
    be released on that same connection. Using pool.acquire() for the unlock
    would target a different backend session and silently fail.
    """
    job_id   = job["id"]
    job_type = job["type"]
    attempt  = job["attempt"]

    await pool.execute("""
        UPDATE jobs
        SET    status       = 'running',
               heartbeat_at = NOW()
        WHERE  id           = $1
    """, job_id)

    logger.info(
        "Worker %s running job %s (type=%s attempt=%s)",
        worker_id, job_id, job_type, attempt,
    )
    t0 = time.monotonic()

    try:
        handler  = get_handler(job_type)
        job_dict = dict(job)
        result   = await handler(job_dict)
        elapsed  = time.monotonic() - t0

        # Pass dict directly — the pool's JSONB codec encodes it for the wire.
        # Passing json.dumps(result) here would double-encode (string → JSON string).
        await pool.execute("""
            UPDATE jobs
            SET    status       = 'done',
                   result       = $1,
                   completed_at = NOW(),
                   worker_id    = $2,
                   heartbeat_at = NULL
            WHERE  id           = $3
        """, result, worker_id, job_id)

        logger.info("Worker %s done job %s in %.3fs", worker_id, job_id, elapsed)

    except Exception as exc:  # noqa: BLE001
        elapsed = time.monotonic() - t0
        logger.warning(
            "Worker %s job %s failed (attempt %s) in %.3fs: %s",
            worker_id, job_id, attempt, elapsed, exc,
        )
        await _handle_failure(pool, job, exc, worker_id)

    finally:
        # Release on claim_conn — the same session that acquired it.
        try:
            await claim_conn.execute("SELECT pg_advisory_unlock($1)", job_id)
        except Exception:  # noqa: BLE001
            pass  # connection closing releases the lock automatically


# ---------------------------------------------------------------------------
# Retry / dead-letter
# ---------------------------------------------------------------------------

async def _handle_failure(
    pool: asyncpg.Pool,
    job: asyncpg.Record,
    error: Exception,
    worker_id: str,
) -> None:
    """Schedule a retry or move the job to dead_letter_jobs."""
    job_id      = job["id"]
    attempt     = job["attempt"]
    max_retries = job["max_retries"]
    error_str   = f"{type(error).__name__}: {error}"

    if attempt + 1 >= max_retries:
        # Atomically insert DLQ row + mark dead so no observer ever sees a
        # state where the job is dead but the DLQ row is missing.
        payload_dict = (
            dict(job["payload"]) if isinstance(job["payload"], dict) else {}
        )
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO dead_letter_jobs
                      (original_id, type, payload, error, attempts, worker_id)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """,
                    job_id,
                    job["type"],
                    payload_dict,          # dict — codec encodes
                    error_str,
                    attempt + 1,
                    worker_id,
                )
                await conn.execute("""
                    UPDATE jobs
                    SET    status  = 'dead',
                           error   = $1,
                           attempt = attempt + 1
                    WHERE  id      = $2
                """, error_str, job_id)

        logger.error("Job %s dead-lettered after %s attempts", job_id, attempt + 1)

    else:
        backoff = (2 ** attempt) * BASE_DELAY
        await pool.execute("""
            UPDATE jobs
            SET    status    = 'pending',
                   attempt   = attempt + 1,
                   run_at    = NOW() + ($1 || ' seconds')::interval,
                   error     = $2,
                   worker_id = NULL
            WHERE  id        = $3
        """, str(backoff), error_str, job_id)

        logger.info("Job %s retry %s in %.0fs", job_id, attempt + 1, backoff)


# ---------------------------------------------------------------------------
# Main worker coroutine
# ---------------------------------------------------------------------------

async def run_worker(
    worker_id: str,
    pool: asyncpg.Pool,
    dsn: Optional[str] = None,
) -> None:
    """
    Single-worker coroutine.

    *dsn* — optional DSN for the two dedicated raw connections (listen_conn
    and claim_conn). Defaults to _db_dsn() (reads DATABASE_URL env var).
    Pass an explicit DSN in tests to target the test database.

    Two dedicated connections are used:
      listen_conn  — holds the LISTEN subscription for job_channel
      claim_conn   — holds advisory locks for the full duration of each job
    Neither is returned to the pool so their session-scoped state is stable.
    """
    resolved_dsn = dsn or _db_dsn()
    notify_event = asyncio.Event()
    _notify_events[worker_id] = notify_event

    listen_conn = await asyncpg.connect(dsn=resolved_dsn)
    await _init_conn(listen_conn)

    claim_conn = await asyncpg.connect(dsn=resolved_dsn)
    await _init_conn(claim_conn)

    cb = _make_notify_callback(worker_id)
    await listen_conn.add_listener("job_channel", cb)

    logger.info("Worker %s started", worker_id)

    try:
        while not shutdown_event.is_set():
            job = await _try_claim(claim_conn, worker_id)

            if job is not None:
                await _execute_job(pool, claim_conn, job, worker_id)
                # Loop immediately — don't sleep after successful execution.
                continue

            # No eligible job: block until notified or poll-fallback fires.
            try:
                await asyncio.wait_for(notify_event.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass
            finally:
                notify_event.clear()

    finally:
        try:
            await listen_conn.remove_listener("job_channel", cb)
        except Exception:  # noqa: BLE001
            pass
        await listen_conn.close()
        await claim_conn.close()
        _notify_events.pop(worker_id, None)
        logger.info("Worker %s shut down cleanly", worker_id)


# ---------------------------------------------------------------------------
# Process entry point
# ---------------------------------------------------------------------------

async def _main() -> None:
    from worker.heartbeat import heartbeat_loop, stale_reaper_loop

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    pool = await create_pool()
    dsn  = _db_dsn()

    loop = asyncio.get_event_loop()

    def _handle_signal() -> None:
        logger.info("Shutdown signal received — draining workers…")
        shutdown_event.set()

    loop.add_signal_handler(signal.SIGTERM, _handle_signal)
    loop.add_signal_handler(signal.SIGINT,  _handle_signal)

    worker_ids = [f"worker-{uuid.uuid4().hex[:8]}" for _ in range(WORKER_COUNT)]

    tasks = [
        asyncio.create_task(run_worker(wid, pool, dsn), name=f"worker-{wid}")
        for wid in worker_ids
    ]
    tasks += [
        asyncio.create_task(heartbeat_loop(wid, pool), name=f"hb-{wid}")
        for wid in worker_ids
    ]
    tasks += [
        asyncio.create_task(stale_reaper_loop(pool), name="stale-reaper"),
    ]

    await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()
    logger.info("All workers exited")


if __name__ == "__main__":
    asyncio.run(_main())
