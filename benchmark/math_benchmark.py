"""
benchmark/math_benchmark.py
----------------------------
Benchmark using mathematically verifiable jobs.
Every result is checked against Python's own computation.

What this proves:
  - Correctness: N/N results match independently computed ground truth
  - Real latency: jobs do actual CPU work (not 0ms noop)
  - Throughput: meaningful under real computational load
  - Zero corruption: payload/result integrity across queue boundaries

Handlers used:
  is_prime     — trial division up to √n  (O(√n) divisions)
  collatz      — 3n+1 sequence to 1       (variable steps, deterministic)
  sha256_chain — SHA-256 chained N times  (configurable CPU load)

Usage (from project root, inside container):
  docker compose exec api python benchmark/math_benchmark.py
  docker compose exec api python benchmark/math_benchmark.py --jobs 500
  docker compose exec api python benchmark/math_benchmark.py --rounds 10000
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import random
import sys
import time
from typing import Any

import asyncpg
import httpx

# ---------------------------------------------------------------------------
# Colours
# ---------------------------------------------------------------------------
_TTY = sys.stdout.isatty()
def _c(code, t): return f"\033[{code}m{t}\033[0m" if _TTY else t
BOLD  = lambda t: _c("1",   t)
DIM   = lambda t: _c("2",   t)
GREEN = lambda t: _c("92",  t)
RED   = lambda t: _c("91",  t)
CYAN  = lambda t: _c("96",  t)
YELLOW= lambda t: _c("93",  t)

# ---------------------------------------------------------------------------
# Ground-truth implementations (independent of worker handlers)
# ---------------------------------------------------------------------------

def ground_truth_is_prime(n: int) -> dict:
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


def ground_truth_collatz(n: int) -> dict:
    cur   = n
    steps = 0
    max_v = n
    total = n
    while cur != 1:
        cur    = cur // 2 if cur % 2 == 0 else 3 * cur + 1
        steps += 1
        max_v  = max(max_v, cur)
        total += cur
    return {"n": n, "steps": steps, "max_value": max_v, "sequence_checksum": total}


def ground_truth_sha256_chain(seed: str, rounds: int) -> dict:
    current = seed.encode()
    for _ in range(rounds):
        current = hashlib.sha256(current).digest()
    return {"seed": seed, "rounds": rounds, "final_hash": current.hex()}


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify_result(job_type: str, payload: dict, result: dict) -> tuple[bool, str]:
    """Returns (correct, reason_if_wrong)."""
    try:
        if job_type == "is_prime":
            expected = ground_truth_is_prime(payload["n"])
            if result.get("is_prime") != expected["is_prime"]:
                return False, f"n={payload['n']}: got is_prime={result.get('is_prime')}, expected {expected['is_prime']}"
            if result.get("divisors_checked") != expected["divisors_checked"]:
                return False, f"n={payload['n']}: divisors_checked mismatch"
            return True, ""

        elif job_type == "collatz":
            expected = ground_truth_collatz(payload["n"])
            for key in ("steps", "max_value", "sequence_checksum"):
                if result.get(key) != expected[key]:
                    return False, f"n={payload['n']}: {key} mismatch (got {result.get(key)}, expected {expected[key]})"
            return True, ""

        elif job_type == "sha256_chain":
            expected = ground_truth_sha256_chain(payload["seed"], payload["rounds"])
            if result.get("final_hash") != expected["final_hash"]:
                return False, f"hash mismatch for seed={payload['seed']} rounds={payload['rounds']}"
            return True, ""

    except Exception as e:
        return False, f"verification error: {e}"

    return False, f"unknown job_type: {job_type}"


# ---------------------------------------------------------------------------
# Job generation
# ---------------------------------------------------------------------------

# Known large primes and composites for is_prime jobs
_PRIMES = [
    104729, 611953, 1299709, 1500007, 3000017,
    5000057, 7368787, 9999991, 15485863, 32452843,
    49979687, 67867967, 86028121, 104395301, 122949829,
]
_COMPOSITES = [
    104730, 611954, 1299710, 1500009, 3000019,
    5000059, 7368789, 9999993, 15485865, 32452845,
    104729 * 2, 611953 * 3, 1299709 * 5, 104729 * 104729,
    999983 * 999979,
    ]

# Collatz numbers with known interesting sequences
_COLLATZ_SEEDS = [
    27, 97, 871, 6171, 77031, 837799, 8400511, 15733191,
    31. , 703, 9663, 77031, 704511, 56991483,
    # Add variety
    *range(100, 200, 7),
    *range(1000, 2000, 37),
    *range(10000, 15000, 113),
]
# Clean up floats that slipped in
_COLLATZ_SEEDS = [int(x) for x in _COLLATZ_SEEDS if x >= 2]


def generate_jobs(n_jobs: int, sha256_rounds: int) -> list[tuple[str, dict]]:
    """Generate a balanced mix of math job types."""
    jobs: list[tuple[str, dict]] = []

    # 1/3 is_prime jobs
    prime_count = n_jobs // 3
    pool = (_PRIMES + _COMPOSITES) * (prime_count // len(_PRIMES + _COMPOSITES) + 1)
    for num in random.sample(pool, prime_count):
        jobs.append(("is_prime", {"n": num}))

    # 1/3 collatz jobs
    collatz_count = n_jobs // 3
    seeds = _COLLATZ_SEEDS * (collatz_count // len(_COLLATZ_SEEDS) + 1)
    for seed in random.sample(seeds, collatz_count):
        jobs.append(("collatz", {"n": seed}))

    # 1/3 sha256_chain jobs
    sha_count = n_jobs - prime_count - collatz_count
    for i in range(sha_count):
        jobs.append(("sha256_chain", {
            "seed":   f"task-queue-bench-{i:06d}",
            "rounds": sha256_rounds,
        }))

    random.shuffle(jobs)
    return jobs


# ---------------------------------------------------------------------------
# HTTP enqueue
# ---------------------------------------------------------------------------

async def enqueue_batch(
        client: httpx.AsyncClient,
        jobs: list[tuple[str, dict]],
        concurrency: int,
) -> list[dict]:
    """Enqueue all jobs with bounded concurrency, return list of job dicts."""
    sem      = asyncio.Semaphore(concurrency)
    results  = [None] * len(jobs)
    errors   = 0

    async def _enqueue(idx: int, job_type: str, payload: dict):
        nonlocal errors
        async with sem:
            try:
                r = await client.post(
                    "/jobs",
                    json={"type": job_type, "payload": payload},
                    timeout=10.0,
                )
                if r.status_code in (200, 201):
                    results[idx] = r.json()
                else:
                    errors += 1
            except Exception:
                errors += 1

    t0 = time.monotonic()
    await asyncio.gather(*[
        _enqueue(i, jt, jp) for i, (jt, jp) in enumerate(jobs)
    ])
    elapsed = time.monotonic() - t0

    valid = [r for r in results if r is not None]
    rate  = len(valid) / elapsed if elapsed > 0 else 0
    print(f"  Enqueued {len(valid):,}/{len(jobs):,} jobs in {elapsed:.1f}s  ({rate:.0f} jobs/s)")
    if errors:
        print(f"  {YELLOW(str(errors))} enqueue errors (HTTP failures)")
    return valid


# ---------------------------------------------------------------------------
# Wait for completion
# ---------------------------------------------------------------------------

async def wait_for_completion(
        conn: asyncpg.Connection,
        job_ids: list[int],
        timeout: float,
        poll: float = 1.0,
) -> dict[int, dict]:
    """Poll until all jobs reach a terminal state. Returns {id: row_dict}."""
    id_set   = set(job_ids)
    done_map : dict[int, dict] = {}
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        rows = await conn.fetch("""
                                SELECT id, type, status, payload, result,
                                       created_at, completed_at,
                                       EXTRACT(EPOCH FROM (completed_at - created_at)) AS latency_s
                                FROM   jobs
                                WHERE  id = ANY($1::bigint[])
                                  AND  status IN ('done', 'dead', 'failed')
                                """, list(id_set - set(done_map)))

        for row in rows:
            done_map[row["id"]] = dict(row)

        remaining = len(id_set) - len(done_map)
        pct       = 100 * len(done_map) / len(id_set)
        print(f"\r  Completed: {len(done_map):,}/{len(id_set):,}  ({pct:.1f}%)", end="", flush=True)

        if not remaining:
            print()
            return done_map

        await asyncio.sleep(poll)

    print()
    print(YELLOW(f"  Timeout after {timeout}s — {len(id_set) - len(done_map)} jobs incomplete"))
    return done_map


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyse(
        enqueued_jobs: list[dict],       # raw enqueue responses (has id, type, payload)
        completed_map: dict[int, dict],  # id → completed row
) -> dict:
    total     = len(enqueued_jobs)
    completed = len(completed_map)
    failed    = sum(1 for r in completed_map.values() if r["status"] != "done")
    timed_out = total - completed

    # Correctness verification
    correct   = 0
    incorrect = 0
    errors_detail: list[str] = []

    for job in enqueued_jobs:
        jid     = job["id"]
        jtype   = job["type"]
        payload = job.get("payload") or {}

        row = completed_map.get(jid)
        if not row or row["status"] != "done":
            continue

        result = row.get("result") or {}
        ok, reason = verify_result(jtype, payload, result)
        if ok:
            correct += 1
        else:
            incorrect += 1
            if len(errors_detail) < 5:
                errors_detail.append(f"  job {jid} ({jtype}): {reason}")

    # Latency
    latencies = [
        r["latency_s"]
        for r in completed_map.values()
        if r.get("latency_s") is not None and r["status"] == "done"
    ]
    latencies.sort()

    def pct(p: float) -> float:
        if not latencies:
            return 0.0
        idx = int(len(latencies) * p / 100)
        return latencies[min(idx, len(latencies) - 1)]

    # Per-type breakdown
    by_type: dict[str, dict] = {}
    for job in enqueued_jobs:
        jt  = job["type"]
        jid = job["id"]
        row = completed_map.get(jid)
        if jt not in by_type:
            by_type[jt] = {"total": 0, "done": 0, "correct": 0, "latencies": []}
        by_type[jt]["total"] += 1
        if row and row["status"] == "done":
            by_type[jt]["done"] += 1
            ok, _ = verify_result(jt, job.get("payload") or {}, row.get("result") or {})
            if ok:
                by_type[jt]["correct"] += 1
            if row.get("latency_s") is not None:
                by_type[jt]["latencies"].append(row["latency_s"])

    return {
        "total":         total,
        "completed":     completed,
        "failed":        failed,
        "timed_out":     timed_out,
        "correct":       correct,
        "incorrect":     incorrect,
        "errors_detail": errors_detail,
        "latencies":     latencies,
        "p50":           pct(50),
        "p75":           pct(75),
        "p95":           pct(95),
        "p99":           pct(99),
        "max_lat":       latencies[-1] if latencies else 0,
        "by_type":       by_type,
    }


def print_report(r: dict, elapsed_total: float):
    correct   = r["correct"]
    done      = r["completed"] - r["failed"]
    accuracy  = 100 * correct / done if done > 0 else 0
    throughput= done / elapsed_total if elapsed_total > 0 else 0

    acc_str = GREEN(f"{accuracy:.4f}%") if accuracy == 100.0 else RED(f"{accuracy:.4f}%")

    W = 55
    def row(label, value, width=W):
        return f"  │  {label:<28} {str(value):<{width-32}}│"

    print(f"\n  ┌{'─'*W}┐")
    print(f"  │{'  Math Benchmark — Verified Results':^{W}}│")
    print(f"  ├{'─'*W}┤")
    print(row("Jobs submitted",        f"{r['total']:,}"))
    print(row("Jobs completed (done)", f"{done:,}"))
    print(row("Jobs failed/timed out", f"{r['failed'] + r['timed_out']:,}"))
    print(f"  ├{'─'*W}┤")
    print(row("✓ Correct results",     GREEN(f"{correct:,}/{done:,}")))
    print(row("✗ Incorrect results",   RED(str(r['incorrect'])) if r['incorrect'] else "0"))
    print(row("Accuracy",              acc_str))
    print(f"  ├{'─'*W}┤")
    print(row("End-to-end throughput", f"{throughput:.0f} jobs/s"))
    print(row("p50 latency",           f"{r['p50']*1000:.1f} ms"))
    print(row("p75 latency",           f"{r['p75']*1000:.1f} ms"))
    print(row("p95 latency",           f"{r['p95']*1000:.1f} ms"))
    print(row("p99 latency",           f"{r['p99']*1000:.1f} ms"))
    print(row("max latency",           f"{r['max_lat']*1000:.1f} ms"))
    print(f"  ├{'─'*W}┤")
    print(f"  │{'  Per-type breakdown':^{W}}│")
    print(f"  ├{'─'*W}┤")

    for jtype, stats in r["by_type"].items():
        lats = sorted(stats["latencies"])
        p50  = lats[len(lats)//2] * 1000 if lats else 0
        p99  = lats[int(len(lats)*0.99)] * 1000 if lats else 0
        acc  = 100 * stats["correct"] / stats["done"] if stats["done"] > 0 else 0
        acc_s= GREEN("100%") if acc == 100 else RED(f"{acc:.1f}%")
        print(f"  │  {jtype:<14} {stats['correct']:>4}/{stats['done']:<4} correct  "
              f"p50={p50:6.1f}ms  p99={p99:6.1f}ms  {'':<2}│")

    print(f"  └{'─'*W}┘")

    if r["errors_detail"]:
        print(f"\n  {RED('First wrong results:')}")
        for e in r["errors_detail"]:
            print(f"  {e}")

    verdict = (
        GREEN("✓ ALL RESULTS MATHEMATICALLY CORRECT")
        if r["incorrect"] == 0 and r["timed_out"] == 0
        else RED("✗ CORRECTNESS FAILURES DETECTED")
    )
    print(f"\n  {verdict}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    parser = argparse.ArgumentParser(description="Math verification benchmark")
    parser.add_argument("--jobs",        type=int, default=300,
                        help="Total jobs (split evenly across 3 types)")
    parser.add_argument("--concurrency", type=int, default=30,
                        help="Concurrent HTTP enqueue workers")
    parser.add_argument("--rounds",      type=int, default=3000,
                        help="SHA-256 chain rounds per job (controls CPU load)")
    parser.add_argument("--timeout",     type=float, default=180.0,
                        help="Max seconds to wait for completion")
    parser.add_argument("--api-url",     default="http://localhost:8000")
    parser.add_argument("--db-url",
                        default=os.environ.get(
                            "DATABASE_URL",
                            "postgresql://taskuser:taskpass@postgres:5432/taskqueue",
                        ))
    args = parser.parse_args()

    print(BOLD(f"\nMath Verification Benchmark"))
    print(DIM(f"  {args.jobs} jobs  ·  sha256 rounds={args.rounds}  ·  concurrency={args.concurrency}"))
    print(DIM(f"  API: {args.api_url}  ·  DB: {args.db_url}\n"))

    # Connectivity
    try:
        async with httpx.AsyncClient(base_url=args.api_url, timeout=5) as probe:
            r = await probe.get("/health")
            assert r.status_code == 200
    except Exception as e:
        print(RED(f"  Cannot reach API: {e}"))
        sys.exit(1)

    try:
        conn = await asyncpg.connect(args.db_url)
        await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads,
                                  schema="pg_catalog")
    except Exception as e:
        print(RED(f"  Cannot reach DB: {e}"))
        sys.exit(1)

    # Generate jobs
    print(f"  Generating {args.jobs} jobs (is_prime / collatz / sha256_chain)…")
    jobs = generate_jobs(args.jobs, args.rounds)
    type_counts = {}
    for jt, _ in jobs:
        type_counts[jt] = type_counts.get(jt, 0) + 1
    for jt, cnt in sorted(type_counts.items()):
        print(f"    {jt:<20} {cnt:>4} jobs")

    # Pre-compute ground truth (to ensure benchmark verifier is fast)
    print(f"\n  Pre-computing ground truth for {args.jobs} jobs…")
    gt_t0 = time.monotonic()
    for jt, payload in jobs:
        if jt == "is_prime":     ground_truth_is_prime(payload["n"])
        elif jt == "collatz":    ground_truth_collatz(payload["n"])
        elif jt == "sha256_chain": ground_truth_sha256_chain(payload["seed"], payload["rounds"])
    gt_elapsed = time.monotonic() - gt_t0
    print(f"  Ground truth computed in {gt_elapsed:.2f}s "
          f"(≈{args.jobs/gt_elapsed:.0f} verifications/s possible)\n")

    # Enqueue
    print(f"  Enqueueing {args.jobs} jobs (concurrency={args.concurrency})…")
    t0 = time.monotonic()
    async with httpx.AsyncClient(base_url=args.api_url, timeout=30) as client:
        enqueued = await enqueue_batch(client, jobs, args.concurrency)

    if not enqueued:
        print(RED("  No jobs enqueued — aborting"))
        await conn.close()
        sys.exit(1)

    job_ids = [j["id"] for j in enqueued]

    # Build payload lookup from enqueued responses
    # (enqueued job has id, type, payload from the API response)
    enqueued_lookup = {j["id"]: j for j in enqueued}

    # Wait for completion
    print(f"\n  Waiting for {len(job_ids)} jobs to complete…")
    completed_map = await wait_for_completion(conn, job_ids, args.timeout)

    elapsed_total = time.monotonic() - t0

    # Re-attach payload to enqueued list (API response includes payload)
    enqueued_with_payload = list(enqueued_lookup.values())

    # Analyse and report
    result = analyse(enqueued_with_payload, completed_map)
    print_report(result, elapsed_total)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())