"""
worker/registry.py
------------------
Maps job *type* strings to async handler coroutines.

A handler receives the full job row (as an asyncpg Record / dict) and must
return a JSON-serialisable dict that becomes the job's `result` column.
Raising any exception triggers the retry / dead-letter logic in worker.py.

Usage
-----
    from worker.registry import register, get_handler

    @register("send_email")
    async def send_email_handler(job: dict) -> dict:
        ...
        return {"delivered": True}

Built-in handlers (useful for tests and smoke-testing):
    • noop         — succeeds immediately, returns {"ok": True}
    • always_fail  — always raises RuntimeError (drives DLQ tests)
    • flaky        — fails on attempt 0 and 1, succeeds on attempt 2
    • slow         — sleeps for payload["sleep_seconds"] (default 1)
    • echo         — returns the payload as the result
"""

import asyncio
import logging
from typing import Callable, Awaitable, Dict, Any

logger = logging.getLogger(__name__)

# type alias
Handler = Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]

_registry: Dict[str, Handler] = {}


def register(job_type: str):
    """Decorator — registers a coroutine function as the handler for *job_type*."""
    def decorator(fn: Handler) -> Handler:
        if job_type in _registry:
            logger.warning("Handler for job type %r is being overwritten", job_type)
        _registry[job_type] = fn
        logger.debug("Registered handler for job type %r", job_type)
        return fn
    return decorator


def get_handler(job_type: str) -> Handler:
    """Return the handler for *job_type* or raise KeyError."""
    if job_type not in _registry:
        raise KeyError(
            f"No handler registered for job type {job_type!r}. "
            f"Registered types: {list(_registry)}"
        )
    return _registry[job_type]


def list_types() -> list[str]:
    """Return all registered job type names."""
    return list(_registry.keys())


# =============================================================================
# Built-in handlers
# =============================================================================

@register("noop")
async def _noop_handler(job: dict) -> dict:
    """Succeeds instantly — useful for load tests and basic integration tests."""
    return {"ok": True}


@register("always_fail")
async def _always_fail_handler(job: dict) -> dict:
    """Always raises — drives dead-letter queue tests."""
    raise RuntimeError(f"always_fail handler: intentional failure on job {job['id']}")


@register("flaky")
async def _flaky_handler(job: dict) -> dict:
    """Fails on attempt 0 and 1, succeeds on attempt 2+.

    Used by test_retry.py to verify that attempt increments and run_at is
    set to a future timestamp after each failure.
    """
    attempt = job.get("attempt", 0)
    if attempt < 2:
        raise RuntimeError(
            f"flaky handler: intentional failure on attempt {attempt}"
        )
    return {"ok": True, "succeeded_on_attempt": attempt}


@register("slow")
async def _slow_handler(job: dict) -> dict:
    """Sleeps for payload['sleep_seconds'] (default 1).

    Useful for testing graceful shutdown and heartbeat behaviour.
    """
    sleep_seconds = float(job.get("payload", {}).get("sleep_seconds", 1))
    await asyncio.sleep(sleep_seconds)
    return {"slept_for": sleep_seconds}


@register("echo")
async def _echo_handler(job: dict) -> dict:
    """Returns the payload verbatim as the result."""
    return dict(job.get("payload", {}))
