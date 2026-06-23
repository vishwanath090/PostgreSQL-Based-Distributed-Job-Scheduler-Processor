"""
benchmark/load.py
-----------------
Async load test: measure task queue throughput and latency.

Usage
-----
# Start docker compose first
docker compose up -d

# Run the benchmark (defaults: 10,000 jobs, 50 concurrent enqueues, 4 workers)
python benchmark/load.py

# Custom run
python benchmark/load.py --jobs 50000 --concurrency 100 --workers 8 --base-url http://localhost:8000

Output
------
Prints a summary table:

  ┌─────────────────────────────────────────────────────┐
  │            Task Queue Benchmark Results              │
  ├─────────────────────────────────────────────────────┤
  │  Jobs enqueued          10,000                      │
  │  Concurrency            50                          │
  │  Enqueue time           4.21 s                      │
  │  Enqueue rate           2,375 jobs/s                │
  │  Worker count           4                           │
  │  Total pipeline time    22.4 s                      │
  │  End-to-end jobs/sec    446 jobs/s                  │
  │                                                     │
  │  Latency (created_at → completed_at)                │
  │    p50    0.84 s                                    │
  │    p75    1.12 s                                    │
  │    p95    2.34 s                                    │
  │    p99    3.87 s                                    │
  │    max    6.21 s                                    │
  └─────────────────────────────────────────────────────┘

Target: > 500 jobs/sec end-to-end with 4 workers on a local machine.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time
from datetime import timezone

import aiohttp
import asyncpg


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Task Queue load benchmark")
    p.add_argument("--jobs",        type=int, default=10_000, help="Total jobs to enqueue")
    p.add_argument("--concurrency", type=int, default=50,     help="Concurrent HTTP enqueue workers")
    p.add_argument("--workers",     type=int, default=4,      help="Expected worker count (informational)")
    p.add_argument("--base-url",    default="http://localhost:8000", help="API base URL")
    p.add_argument("--db-url",      default="postgresql://taskuser:taskpass@localhost:5432/taskqueue",
                                    help="Direct DB URL for latency sampling")
    p.add_argument("--poll-interval", type=float, default=2.0, help="Seconds between completion polls")
    p.add_argument("--timeout",     type=float, default=120.0, help="Max seconds to wait for completion")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Enqueue phase
# ---------------------------------------------------------------------------

async def _enqueue_batch(
    session: aiohttp.ClientSession,
    base_url: str,
    job_ids: list[int],
    sem: asyncio.Semaphore,
    n: int,
) -> None:
    async with sem:
        async with session.post(
            f"{base_url}/jobs",
            json={"type": "noop", "payload": {"seq": n}},
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            job_ids.append(data["id"])


async def enqueue_phase(base_url: str, total: int, concurrency: int) -> tuple[list[int], float]:
    """
    POST /jobs for `total` jobs with `concurrency` parallel requests.
    Returns (list_of_job_ids, elapsed_seconds).
    """
    sem = asyncio.Semaphore(concurrency)
    job_ids: list[int] = []

    connector = aiohttp.TCPConnector(limit=concurrency + 10)
    async with aiohttp.ClientSession(connector=connector) as session:
        t0 = time.monotonic()
        tasks = [
            asyncio.create_task(_enqueue_batch(session, base_url, job_ids, sem, i))
            for i in range(total)
        ]
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - t0

    return job_ids, elapsed


# ---------------------------------------------------------------------------
# Completion poll phase
# ---------------------------------------------------------------------------

async def wait_for_completion(
    db_url: str,
    job_ids: list[int],
    poll_interval: float,
    timeout: float,
) -> float:
    """
    Poll the DB directly (not the API) to avoid adding HTTP overhead to the
    measurement.  Returns seconds until all jobs reached a terminal status.
    """
    import json

    async def _init(conn):
        await conn.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3, init=_init)
    t0 = time.monotonic()

    try:
        deadline = t0 + timeout
        while time.monotonic() < deadline:
            done_count = await pool.fetchval(
                """
                SELECT COUNT(*) FROM jobs
                WHERE  id = ANY($1::bigint[])
                  AND  status IN ('done', 'dead', 'failed')
                """,
                job_ids,
            )
            pct = done_count / len(job_ids) * 100
            print(
                f"\r  Completed: {done_count:,}/{len(job_ids):,} ({pct:.1f}%)",
                end="",
                flush=True,
            )
            if done_count == len(job_ids):
                print()  # newline after progress
                return time.monotonic() - t0
            await asyncio.sleep(poll_interval)

        print()
        print(
            f"  WARNING: timed out after {timeout}s — "
            f"only {done_count}/{len(job_ids)} jobs completed"
        )
        return time.monotonic() - t0
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Latency sampling
# ---------------------------------------------------------------------------

async def sample_latencies(db_url: str, job_ids: list[int]) -> list[float]:
    """
    Sample end-to-end latency (completed_at - created_at) for all done jobs.
    """
    import json

    async def _init(conn):
        await conn.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )

    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2, init=_init)
    try:
        rows = await pool.fetch(
            """
            SELECT EXTRACT(EPOCH FROM (completed_at - created_at)) AS lat
            FROM   jobs
            WHERE  id = ANY($1::bigint[])
              AND  status = 'done'
              AND  completed_at IS NOT NULL
            """,
            job_ids,
        )
        return [float(r["lat"]) for r in rows]
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _pct(data: list[float], p: float) -> float:
    return statistics.quantiles(data, n=100)[int(p) - 1]


def _print_report(
    total_jobs: int,
    concurrency: int,
    worker_count: int,
    enqueue_elapsed: float,
    pipeline_elapsed: float,
    latencies: list[float],
) -> None:
    enqueue_rate   = total_jobs / enqueue_elapsed if enqueue_elapsed else 0
    e2e_rate       = len(latencies) / pipeline_elapsed if pipeline_elapsed else 0

    cols = 55
    border = "─" * cols
    fmt_num = lambda n: f"{n:,.0f}"

    def row(label: str, value: str) -> str:
        return f"  │  {label:<28}{value:<{cols - 34}}│"

    lines = [
        f"  ┌{border}┐",
        f"  │{'Task Queue Benchmark Results':^{cols}}│",
        f"  ├{border}┤",
        row("Jobs enqueued",         fmt_num(total_jobs)),
        row("Concurrency",           str(concurrency)),
        row("Enqueue time",          f"{enqueue_elapsed:.2f} s"),
        row("Enqueue rate",          f"{enqueue_rate:,.0f} jobs/s"),
        row("Worker count",          str(worker_count)),
        row("Total pipeline time",   f"{pipeline_elapsed:.1f} s"),
        row("End-to-end jobs/sec",   f"{e2e_rate:,.0f} jobs/s"),
        f"  │{'':^{cols}}│",
    ]

    if latencies:
        lines += [
            f"  │  {'Latency (created_at → completed_at)':<{cols - 4}}│",
            row("  p50", f"{_pct(latencies, 50):.3f} s"),
            row("  p75", f"{_pct(latencies, 75):.3f} s"),
            row("  p95", f"{_pct(latencies, 95):.3f} s"),
            row("  p99", f"{_pct(latencies, 99):.3f} s"),
            row("  max", f"{max(latencies):.3f} s"),
            row("  Sampled jobs",     fmt_num(len(latencies))),
        ]

        target_met = e2e_rate >= 500
        verdict = "✓ TARGET MET (≥500 jobs/s)" if target_met else "✗ Below 500 jobs/s target"
        lines += [
            f"  │{'':^{cols}}│",
            f"  │  {verdict:<{cols - 4}}│",
        ]

    lines.append(f"  └{border}┘")
    print("\n" + "\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> int:
    args = _parse()

    print(f"\n  Enqueueing {args.jobs:,} jobs (concurrency={args.concurrency}) …")
    job_ids, enqueue_elapsed = await enqueue_phase(
        args.base_url, args.jobs, args.concurrency
    )
    print(
        f"  Enqueued {len(job_ids):,} jobs in {enqueue_elapsed:.2f}s "
        f"({len(job_ids)/enqueue_elapsed:,.0f} jobs/s)"
    )

    print(f"\n  Waiting for {len(job_ids):,} jobs to complete …")
    pipeline_elapsed = await wait_for_completion(
        args.db_url, job_ids, args.poll_interval, args.timeout
    )

    print("\n  Sampling latencies …")
    latencies = await sample_latencies(args.db_url, job_ids)

    _print_report(
        total_jobs=args.jobs,
        concurrency=args.concurrency,
        worker_count=args.workers,
        enqueue_elapsed=enqueue_elapsed,
        pipeline_elapsed=pipeline_elapsed,
        latencies=latencies,
    )

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
