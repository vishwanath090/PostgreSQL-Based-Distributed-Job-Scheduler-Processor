# Distributed Task Queue — PostgreSQL-native, no Redis

A production-grade background job queue built entirely on PostgreSQL.  No
Redis, no Celery, no external message broker — just asyncpg, FastAPI, and
careful use of PostgreSQL primitives to deliver exactly-once, ordered, retried
job execution at hundreds of jobs per second on a single database host.

---

## What this is

This is a background task queue where job producers (the REST API) and job
consumers (async workers) communicate exclusively through a PostgreSQL database.
The API enqueues jobs via a simple `POST /jobs`, workers claim and execute them,
and the database keeps a durable, queryable record of every job's lifecycle from
`pending` through `done` or into the dead-letter queue.  The design deliberately
avoids external brokers: PostgreSQL's `LISTEN/NOTIFY` replaces Redis Pub/Sub for
push-driven wakeup, `SELECT … FOR UPDATE SKIP LOCKED` replaces queue-primitive
atomics, and session-scoped advisory locks provide a hold-duration that outlasts
any single transaction.  The result is a system that is easy to operate (one
fewer moving part), fully ACID-consistent (no "at-least-once" approximations
without compensating logic), and introspectable with ordinary SQL.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client / Application                        │
└────────────────────────────┬────────────────────────────────────────┘
                             │ HTTP REST
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         FastAPI (api/)                              │
│                                                                     │
│  POST /jobs ──► INSERT INTO jobs                                    │
│                 └─► pg_notify('job_channel', id)  [via DB trigger]  │
│  GET  /jobs/{id}  ──► SELECT * FROM jobs WHERE id=$1               │
│  GET  /metrics    ──► aggregate queries                             │
└────────────────────────────┬────────────────────────────────────────┘
                             │ asyncpg pool
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        PostgreSQL                                   │
│                                                                     │
│  jobs              ◄──── SKIP LOCKED claim (workers)               │
│  dead_letter_jobs  ◄──── atomic insert on retry exhaustion          │
│                                                                     │
│  trg_notify_new_job ──► pg_notify on every INSERT                  │
│  idx_jobs_queue     ──► partial index (pending only, priority DESC) │
│  idx_jobs_claimed   ──► partial index (claimed/running only)        │
└────────────┬──────────────────────────────────────────────────────-─┘
             │ LISTEN 'job_channel'      │ SELECT … SKIP LOCKED
             │ (dedicated connection)    │ + pg_try_advisory_lock
             ▼                          ▼
┌────────────────────────────────────────────────────────────────────┐
│                   Worker Pool (worker/)                            │
│                                                                    │
│  worker_0   worker_1   worker_2   worker_3                         │
│     │           │          │          │                            │
│     └──────── notify_event (asyncio.Event) ◄── NOTIFY callback    │
│                                                                    │
│  heartbeat_loop  ── UPDATE heartbeat_at every 10s                 │
│  stale_reaper    ── reset dead workers' jobs every 15s            │
└────────────────────────────────────────────────────────────────────┘
```

---

## How exactly-once execution works

**The problem.**  Naive `SELECT … WHERE status='pending'` + `UPDATE … SET
status='claimed'` breaks under concurrency: two workers can read the same row
before either writes the update, executing the same job twice.  This is the
classic TOCTOU race.

**Lock 1 — `FOR UPDATE SKIP LOCKED` (row-level, transaction-scoped).**
Inside a transaction each worker's `SELECT` acquires an exclusive row lock on
every candidate row it reads.  `SKIP LOCKED` means a worker transparently skips
any row already locked by another transaction rather than blocking on it.  Under
high concurrency this eliminates both the race (no two workers can hold the
exclusive lock on the same row simultaneously) and queue head-of-line blocking
(a slow worker doesn't stall the entire field).  The lock is released when the
claim transaction commits — at which point `status` is already `'claimed'` so
the row is no longer in the `WHERE status='pending'` predicate.

**Lock 2 — `pg_try_advisory_lock(id)` (session-level, connection-scoped).**
`FOR UPDATE SKIP LOCKED` protects the row only for the duration of the claim
transaction.  The moment that transaction commits, the row lock is released.
Between "commit" and "handler returns" another worker running a new claim
transaction could re-select the same row (its status is `'claimed'`, not
`'pending'`, so that worker won't; but a crash-recovery scenario via the stale
reaper could reset it to `'pending'` prematurely if heartbeats are configured
too aggressively).  The advisory lock closes this window: `pg_try_advisory_lock`
is non-blocking, returns `FALSE` immediately if another session already holds
the lock, and the lock persists until `pg_advisory_unlock` is called or the
*connection* closes — whichever comes first.  A worker crash therefore releases
the lock automatically, preventing orphaned locks.

**Why both are needed.**  Either alone is insufficient.  `SKIP LOCKED` alone
provides no protection after the claim transaction commits.  Advisory locks alone
provide no protection against two workers calling `pg_try_advisory_lock` before
either calls the `UPDATE` (a window the transaction eliminates by making the
read-lock-update atomic).  Together they form a two-phase protection: SKIP LOCKED
prevents the race at claim time; the advisory lock prevents re-claim during
execution.

---

## Why no Redis

Redis is excellent, but adding it as a required component means: one more
service to provision, monitor, backup, and reason about during incidents; a
second failure domain (Redis outage → no job dispatch); and an eventual
consistency gap between the Redis stream offset and PostgreSQL row state that
requires careful reconciliation logic.  This queue stores jobs in PostgreSQL
from the start, so all durability, ordering, and exactly-once guarantees live
in one place with full ACID transactions.  The trade-off is throughput ceiling
(a single Postgres host tops out around 5,000–10,000 enqueues/second depending
on hardware, versus Redis's ~100,000+) and the fact that `LISTEN/NOTIFY` has
no persistence guarantee — a notify fired during a worker restart is lost, hence
the 5-second polling fallback.  For the workloads this queue targets (hundreds
to low thousands of jobs per second with strong durability requirements), that
trade-off is the right call.

---

## Running locally

```bash
git clone https://github.com/you/task-queue
cd task-queue

# Start everything (Postgres + API + 1 worker process with 4 internal workers)
docker compose up --build

# API is now at http://localhost:8000
# Interactive docs: http://localhost:8000/docs

# Enqueue a job
curl -s -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"noop","priority":8}' | jq .

# Check status
curl -s http://localhost:8000/jobs/1 | jq .status

# Metrics
curl -s http://localhost:8000/metrics | jq .

# Scale workers to 3 processes (each runs WORKER_COUNT=4 goroutines = 12 total)
docker compose up --scale worker=3

# Graceful shutdown (workers finish current job before exiting)
docker compose stop worker
```

---

## Running tests

Tests require a running PostgreSQL instance.  The default config connects to
`localhost:5432` with the credentials in `docker-compose.yml`.

```bash
# Option A: use docker compose (simplest)
docker compose up -d postgres
pip install -r requirements.txt
pytest tests/ -v

# Option B: full in-compose run
docker compose run --rm api pytest tests/ -v

# Expected output (abridged):
# tests/test_enqueue.py::test_enqueue_minimal_body            PASSED
# tests/test_enqueue.py::test_enqueue_full_body               PASSED
# tests/test_enqueue.py::test_cancel_pending_job              PASSED
# tests/test_enqueue.py::test_cancel_running_job_returns_409  PASSED
# tests/test_exactly_once.py::test_exactly_once_single_job    PASSED
# tests/test_exactly_once.py::test_exactly_once_many_jobs     PASSED
# tests/test_exactly_once.py::test_no_job_claimed_twice_...   PASSED
# tests/test_retry.py::test_retry_sets_run_at_to_future       PASSED
# tests/test_retry.py::test_full_retry_cycle_flaky_job        PASSED
# tests/test_retry.py::test_backoff_delay_formula             PASSED
# tests/test_dlq.py::test_dlq_job_lands_after_exhausted_...  PASSED
# tests/test_dlq.py::test_dlq_transition_is_atomic            PASSED
# tests/test_priority.py::test_priority_order_single_worker   PASSED
# tests/test_priority.py::test_try_claim_returns_highest_...  PASSED
# tests/test_priority.py::test_future_scheduled_job_not_...   PASSED
# tests/test_stale_reaper.py::test_stale_running_job_...      PASSED
# tests/test_stale_reaper.py::test_end_to_end_reaper_then_... PASSED
# ================================================================
# 25 passed in 18.42s
```

---

## Benchmark results

Run on a 2023 MacBook Pro M2, Docker Desktop, 4 worker goroutines, PostgreSQL 16
default config (no tuning):

```
  ┌───────────────────────────────────────────────────────┐
  │           Task Queue Benchmark Results                │
  ├───────────────────────────────────────────────────────┤
  │  Jobs enqueued          10,000                       │
  │  Concurrency            50                           │
  │  Enqueue time           3.84 s                       │
  │  Enqueue rate           2,604 jobs/s                 │
  │  Worker count           4                            │
  │  Total pipeline time    18.7 s                       │
  │  End-to-end jobs/sec    535 jobs/s                   │
  │                                                      │
  │  Latency (created_at → completed_at)                 │
  │    p50    0.71 s                                     │
  │    p75    0.98 s                                     │
  │    p95    2.11 s                                     │
  │    p99    3.44 s                                     │
  │    max    5.88 s                                     │
  │                                                      │
  │  ✓ TARGET MET (≥500 jobs/s)                          │
  └───────────────────────────────────────────────────────┘
```

To reproduce:

```bash
docker compose up -d
python benchmark/load.py --jobs 10000 --concurrency 50 --workers 4
```

---

## Known limitations

**`LISTEN/NOTIFY` delivery is not guaranteed.**  `pg_notify` is best-effort:
notifications emitted during a worker restart are silently dropped.  The 5-second
polling fallback handles this, but it means worst-case dispatch latency is 5 s,
not ~0 ms.  A dedicated broker (Kafka, SQS) persists notifications to disk.

**Single PostgreSQL host is the bottleneck.**  Every enqueue, claim, heartbeat,
and result write hits the same Postgres instance.  At ~5,000 enqueues/second the
WAL and connection overhead become the ceiling.  Horizontal scaling requires
partitioning the `jobs` table by hash(id) and routing workers to specific
partitions — non-trivial with advisory locks (which are global, not
partition-scoped).

**PgBouncer + advisory locks don't mix.**  Advisory locks are connection-scoped.
If PgBouncer is deployed in transaction-pooling mode (the efficient mode), each
`pg_try_advisory_lock` call may land on a different backend connection than the
subsequent `pg_advisory_unlock`, which will silently fail to release the lock.
Use session-pooling mode with PgBouncer, or skip PgBouncer entirely and rely on
asyncpg's built-in pool (which holds connections open, preserving the
session-scoped lock).

**No backpressure.**  `POST /jobs` succeeds immediately regardless of queue
depth.  Under extreme load (millions of pending jobs) the partial index on
`status='pending'` degrades and claim latency rises.  An admission-control
layer (reject enqueues above N pending) and table partitioning would fix this.

**Heartbeat window vs. stale threshold.**  The default STALE_THRESHOLD (30 s)
is two heartbeat cycles (2 × 10 s) plus a 10 s safety margin.  A GC pause or
overloaded host can cause a healthy worker to miss its heartbeat window and have
its job reclaimed.  If the original worker then also completes the job you get
a double-write to the result column (last writer wins — not dangerous for noop
jobs but worth auditing for side-effecting handlers).

---

## What I'd do differently at scale

1. **Dedicated broker for fan-out.**  Replace `LISTEN/NOTIFY` with Kafka or SQS
   for guaranteed delivery, consumer group semantics, and horizontal topic
   partitioning.  PostgreSQL remains the source of truth for job state, but
   the broker handles routing.

2. **Partition the jobs table.**  `PARTITION BY HASH(id)` across 8–16 child
   tables lets workers be pinned to specific partitions, reducing lock contention
   and index size.  Each partition's partial index stays small and fast.

3. **Read replica for status polling.**  `GET /jobs/{id}` and `GET /jobs` don't
   need the primary.  Routing these to a read replica halves the primary's read
   load and keeps the primary focused on writes and advisory locks.

4. **Separate claim connection from execution connection.**  The current design
   acquires the advisory lock on a pool connection and then uses the pool for
   execution.  Under PgBouncer session pooling this works but wastes a backend
   connection during long-running jobs.  A dedicated "claim" connection per
   worker (separate from the shared pool) keeps advisory lock semantics correct
   without tying up pool slots.

5. **Prometheus + Grafana stack.**  The `/metrics/prometheus` endpoint is
   already Prometheus-compatible.  In production: scrape every 15 s, alert on
   `taskqueue_queue_depth{status="pending"} > 10000` and
   `taskqueue_execution_seconds{quantile="0.99"} > 30`.
