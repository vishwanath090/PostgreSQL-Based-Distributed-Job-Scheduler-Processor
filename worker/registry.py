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


# =============================================================================
# Mathematical computation handlers — results are verifiable correct
# =============================================================================

@register("is_prime")
async def _is_prime_handler(job: dict) -> dict:
    """
    Trial-division primality test.

    Payload:  {"n": 15485863}
    Result:   {"n": 15485863, "is_prime": true, "divisors_checked": 3936}

    Verification: recompute with the same algorithm and compare.
    Real CPU work: O(√n) divisions — takes measurable time for large n.
    """
    import math
    n = int(job["payload"]["n"])

    if n < 2:
        return {"n": n, "is_prime": False, "divisors_checked": 0}
    if n == 2:
        return {"n": n, "is_prime": True,  "divisors_checked": 1}
    if n % 2 == 0:
        return {"n": n, "is_prime": False, "divisors_checked": 1}

    checked = 1
    limit   = math.isqrt(n) + 1
    for d in range(3, limit, 2):
        checked += 1
        if n % d == 0:
            return {"n": n, "is_prime": False, "divisors_checked": checked}

    return {"n": n, "is_prime": True, "divisors_checked": checked}


@register("collatz")
async def _collatz_handler(job: dict) -> dict:
    """
    Compute the Collatz (3n+1) sequence from n until it reaches 1.

    Payload:  {"n": 27}
    Result:   {"n": 27, "steps": 111, "max_value": 9232, "sequence_checksum": ...}

    The sequence_checksum (sum of all values) is deterministic and verifiable.
    """
    n   = int(job["payload"]["n"])
    cur = n
    steps     = 0
    max_val   = n
    total_sum = n                    # checksum: sum of every value in the sequence

    while cur != 1:
        if cur % 2 == 0:
            cur //= 2
        else:
            cur = 3 * cur + 1
        steps     += 1
        max_val    = max(max_val, cur)
        total_sum += cur

    return {
        "n":                  n,
        "steps":              steps,
        "max_value":          max_val,
        "sequence_checksum":  total_sum,
    }


@register("sha256_chain")
async def _sha256_chain_handler(job: dict) -> dict:
    """
    Hash a seed string through SHA-256 `rounds` times in a chain.
    Each round hashes the previous hex digest.

    Payload:  {"seed": "task-queue", "rounds": 5000}
    Result:   {"final_hash": "abc123...", "rounds": 5000, "seed": "task-queue"}

    Deterministic: same seed + rounds always produces the same final_hash.
    Tunable CPU load: increase rounds for heavier work.
    """
    import hashlib
    seed   = str(job["payload"]["seed"])
    rounds = int(job["payload"].get("rounds", 1000))

    current = seed.encode()
    for _ in range(rounds):
        current = hashlib.sha256(current).digest()

    return {
        "seed":       seed,
        "rounds":     rounds,
        "final_hash": current.hex(),
    }