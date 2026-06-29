# Distributed Task Queue — PostgreSQL-native, no Redis

**A production-grade background job queue that runs entirely on PostgreSQL.**
No Redis. No Celery. No external broker. Just `asyncpg`, FastAPI, and a
deliberate, well-tested use of PostgreSQL's own concurrency primitives to
get exactly-once, priority-ordered, retried job execution out of a database
most teams are already running.

> Stack: Python 3.12 · FastAPI · asyncpg · PostgreSQL 16 · zero external brokers

---

## The idea

Most task-queue tutorials start from "spin up Redis." That's a reasonable
default, but it's also a second system to provision, patch, monitor, and
reason about during an incident — and a second source of truth that can
drift out of sync with whatever's in your actual database.

This project asks a narrower question: **if your job metadata already needs
to live in PostgreSQL for durability and queryability, how far can Postgres
alone take you as the queue itself?** The answer, it turns out, is
surprisingly far. `SELECT … FOR UPDATE SKIP LOCKED` gives you the
atomic-claim primitive other queues build into a broker. Session-scoped
advisory locks give you a hold that outlives a single transaction.
`LISTEN/NOTIFY` gives you push-based wakeup instead of polling. None of it
is exotic — it's all stock PostgreSQL — but composed carefully, it adds up
to a queue with **one fewer moving part** and **no eventual-consistency gap**
between "what the broker thinks happened" and "what the database says
happened," because there's only one ledger.

The trade-off is throughput ceiling: a single Postgres host won't touch
Redis's six-figure ops/second. For workloads in the hundreds-to-low-thousands
of jobs per second with strong durability and operational simplicity as
priorities, that trade-off is the right one — and this repo backs that claim
with a benchmark that doesn't just measure speed, it independently
**re-derives and checks every single result** (see [Benchmarks](#benchmarks)).

---

## What's inside

- **Exactly-once execution** — two complementary PostgreSQL locks close every
  race window between "claimed" and "running" (full mechanics below).
- **Priority + scheduled jobs** — `priority` (1–10) and `run_at` let you
  queue work for later or push it to the front of the line.
- **Automatic retries with exponential backoff**, then a **dead-letter
  queue** once `max_retries` is exhausted — with the failing job's full
  payload, error, and attempt history preserved for replay or audit.
- **Push-driven dispatch** via `LISTEN/NOTIFY`, with a 5-second polling
  fallback so a notification dropped during a restart never stalls a job
  indefinitely.
- **Heartbeats + a stale-job reaper** that reclaims work from crashed or
  hung workers without any manual intervention.
- **A pluggable handler registry** — register a new job type with one
  decorator; eight handlers (including three that do real, independently
  verifiable computation) ship out of the box.
- **JSON and Prometheus metrics endpoints** for queue depth, throughput,
  error rate, and execution latency.
- **35 integration tests against a real PostgreSQL instance** — no mocking
  of the database layer at all.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client / Application                        │
└────────────────────────────┬──────────────────────────────────────-─┘
                              │ HTTP REST
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         FastAPI  (api/)                              │
│                                                                      │
│  POST   /jobs        ──► INSERT INTO jobs                            │
│                          └─► pg_notify('job_channel', id)  [trigger]  │
│  GET    /jobs/{id}   ──► poll status + result                        │
│  GET    /jobs        ──► list, filter by status/type/created_after   │
│  DELETE /jobs/{id}   ──► cancel — only while still 'pending'          │
│  GET    /metrics     ──► JSON: depth, throughput, error rate, latency │
│  GET    /metrics/prometheus ──► Prometheus exposition format          │
│  GET    /health      ──► liveness probe                               │
└────────────────────────────┬──────────────────────────────────────-─┘
                              │ asyncpg pool
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          PostgreSQL                                  │
│                                                                      │
│  jobs               ◄──── SKIP LOCKED claim (workers)                │
│  dead_letter_jobs   ◄──── atomic insert on retry exhaustion          │
│                                                                      │
│  trg_notify_new_job ──► pg_notify on every INSERT                    │
│  idx_jobs_queue     ──► partial index (pending only, priority DESC)  │
│  idx_jobs_claimed   ──► partial index (claimed/running only)         │
└────────────┬──────────────────────────────────────────────────────-─┘
             │ LISTEN 'job_channel'        │ SELECT … SKIP LOCKED
             │ (dedicated connection)      │ + pg_try_advisory_lock
             ▼                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      Worker Pool  (worker/)                          │
│                                                                       │
│  worker_0    worker_1    worker_2    worker_3      (WORKER_COUNT=4)   │
│     │           │           │           │                            │
│     └───────── notify_event (asyncio.Event) ◄── NOTIFY callback       │
│                                                                       │
│  heartbeat_loop   ── touches heartbeat_at every 10s                  │
│  stale_reaper     ── resets dead workers' jobs to 'pending' every 15s│
└───────────────────────────────────────────────────────────────────────┘
```

Each worker holds two **dedicated, never-pooled** connections: one purely
for `LISTEN`, one purely for claiming and holding advisory locks for the
full lifetime of a job. That separation is deliberate — see
[PgBouncer + advisory locks don't mix](#known-limitations) below for why
mixing them with a shared pool would silently break correctness.

---

## How exactly-once execution works

**The problem.** A naive `SELECT … WHERE status='pending'` followed by
`UPDATE … SET status='claimed'` breaks under concurrency: two workers can
read the same row before either writes its update, and both go on to
execute the same job. It's the classic TOCTOU race, and it's exactly the
kind of bug that only shows up under load.

**Lock 1 — `FOR UPDATE SKIP LOCKED`** *(row-level, transaction-scoped)*.
Inside a transaction, each worker's `SELECT` takes an exclusive row lock on
every candidate row it reads. `SKIP LOCKED` means a worker silently skips
any row another transaction already has locked, instead of blocking on it.
Under concurrency this kills two birds at once: no two workers can ever hold
the lock on the same row simultaneously, and a slow worker never stalls the
whole queue (no head-of-line blocking). The lock releases the moment the
claim transaction commits — by which point `status` is already `'claimed'`,
so the row has already dropped out of the `WHERE status='pending'`
predicate that every other worker is scanning.

**Lock 2 — `pg_try_advisory_lock(id)`** *(session-level, connection-scoped)*.
`SKIP LOCKED` only protects the row for the duration of the claim
transaction. The instant that transaction commits, the row lock is gone.
Between "commit" and "handler finishes," nothing in the row's `status` column
stops a second claim — under normal operation the row reads `'claimed'`, not
`'pending'`, so other workers correctly skip it; but if the stale-reaper's
threshold is tuned too aggressively relative to the heartbeat interval, it
could reset that row to `'pending'` while the original worker is still
mid-execution. The advisory lock closes exactly that window:
`pg_try_advisory_lock` is non-blocking, returns `FALSE` immediately if
another session already holds it, and — critically — it persists until
`pg_advisory_unlock` is called *or the connection closes*, whichever happens
first. A crashed worker therefore releases its own lock automatically the
instant its connection drops. No orphaned locks, no manual cleanup.

**Why you need both.** Either lock alone has a hole. `SKIP LOCKED` alone
gives you nothing once the claim transaction commits. The advisory lock
alone gives you nothing during the brief window before either worker has
called it — two workers could both reach `pg_try_advisory_lock` before
either has run the `UPDATE`. The transaction is what makes
read-lock-then-update atomic in the first place. Together, they form a
two-phase guarantee: `SKIP LOCKED` prevents the race *at the moment of
claiming*; the advisory lock prevents a re-claim *for as long as the job is
running*. The implementation enforces this by acquiring and releasing both
locks on the exact same physical connection (`claim_conn`) for the life of
the job — never on a connection borrowed from the shared pool, since
advisory locks belong to the session that took them.

---

## API reference

| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs` | Enqueue a job. Body: `type`, `payload` (any JSON), `priority` (1–10, default 5), `max_retries` (0–10, default 3), `run_at` (optional ISO-8601 — omit for "now"). |
| `GET` | `/jobs/{id}` | Fetch a job's current status, attempt count, and result/error. |
| `GET` | `/jobs` | List jobs. Filters: `status`, `type`, `created_after`; paginated with `limit`/`offset`. |
| `DELETE` | `/jobs/{id}` | Cancel a job. Only works while it's still `pending` — returns `409` if it's already claimed or running. |
| `GET` | `/metrics` | JSON: queue depth by status, active workers, jobs/sec (trailing 60s), error rate, average execution time, dead-letter count. |
| `GET` | `/metrics/prometheus` | The same metrics in Prometheus exposition format. |
| `GET` | `/health` | Liveness probe used by the Docker healthcheck. |

Interactive docs are auto-generated by FastAPI at `/docs` once the API is
running.

---

## Job lifecycle and schema

```
pending ──► claimed ──► running ──┬──► done
                                   ├──► pending   (retry, run_at pushed forward)
                                   └──► dead      (max_retries exhausted → dead_letter_jobs)
```

The `jobs` table (see `db/migrations/001_init.sql`) carries everything
needed to answer "what happened to job 4017?" with a single `SELECT` — no
side channel required: `id`, `type`, `payload`, `status`, `priority`,
`attempt`, `max_retries`, `run_at`, `claimed_at`, `heartbeat_at`,
`completed_at`, `result`, `error`, `worker_id`, `created_at`. A `status`
`CHECK` constraint stands in for an enum (keeps migrations simpler), and two
partial indexes — one on pending rows ordered by `priority DESC, run_at ASC`,
one on claimed/running rows by `heartbeat_at` — keep the two hot paths
(claiming, reaping) fast regardless of how large the `done`/`dead` history
grows.

Jobs that exhaust their retries are moved, in the same transaction as the
status update, into `dead_letter_jobs` with their original payload, final
error, attempt count, and the worker that last touched them — so an
operator can inspect, replay, or discard them deliberately rather than
losing the failure context.

---

## Built-in job handlers

Registering a new job type is one decorator (`worker/registry.py`):

```python
from worker.registry import register

@register("send_email")
async def send_email_handler(job: dict) -> dict:
    ...
    return {"delivered": True}
```

Eight handlers ship with the repo, mostly for testing and demonstration —
three of them do real, independently checkable computation rather than just
sleeping, which is what makes the benchmark below meaningful:

| Type | Behavior |
|---|---|
| `noop` | Succeeds instantly. |
| `echo` | Returns the payload unchanged. |
| `slow` | Sleeps for `payload.sleep_seconds` — useful for exercising graceful shutdown and heartbeats. |
| `always_fail` | Always raises — drives the dead-letter-queue tests. |
| `flaky` | Fails on attempts 0 and 1, succeeds on attempt 2 — drives the retry tests. |
| `is_prime` | Trial-division primality test, O(√n); result includes `divisors_checked`. |
| `collatz` | Computes the 3n+1 sequence to 1; result includes step count and a running checksum. |
| `sha256_chain` | Chains SHA-256 over a seed `rounds` times — tunable, deterministic CPU load. |

---

## Why no Redis

Redis is a fine piece of software. Making it a *required* component of a
job queue means: one more service to provision, monitor, back up, and
reason about during an incident; a second failure domain (Redis down means
no dispatch, independent of whether Postgres is healthy); and a
reconciliation problem between "what the Redis stream offset says" and
"what the Postgres row says" that most implementations handle with
best-effort glue rather than a real guarantee.

Keeping everything in Postgres from the start means durability, ordering,
and exactly-once semantics live in one place, inside real ACID transactions
— nothing to reconcile because there's only one source of truth. The honest
cost: a single Postgres host tops out somewhere in the 5,000–10,000
enqueues/second range depending on hardware, well short of Redis's
six-figure throughput, and `LISTEN/NOTIFY` itself gives no delivery
guarantee — a notification fired while a worker is mid-restart is simply
lost, which is exactly why the 5-second polling fallback exists. For
workloads in the hundreds-to-low-thousands of jobs/second with strong
durability requirements, that's the right trade to make.

---

## Quickstart

```bash
git clone https://github.com/vishwanath090/PostgreSQL-Based-Distributed-Job-Scheduler-Processor.git
cd PostgreSQL-Based-Distributed-Job-Scheduler-Processor

# Postgres + API + one worker process (WORKER_COUNT=4 internal workers)
docker compose up --build

# API: http://localhost:8000
# Interactive docs: http://localhost:8000/docs

# Enqueue a job
curl -s -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"noop","priority":8}' | jq .

# Poll its status
curl -s http://localhost:8000/jobs/1 | jq .status

# Check queue health
curl -s http://localhost:8000/metrics | jq .

# Scale to 3 worker processes (12 internal workers total)
docker compose up --scale worker=3

# Graceful shutdown — SIGTERM is caught, current job finishes (60s grace period)
docker compose stop worker
```

Both containers run as a non-root user, and the `api` service has a Docker
healthcheck hitting `/health` so `depends_on: condition: service_healthy`
actually means something.

---

## Tooling

Beyond the API and workers, the repo includes a small set of operational
scripts that are worth knowing about:

- **`scripts/run_tests.sh`** — a test runner with named modes, so you don't
  have to remember pytest flags:
  ```bash
  bash scripts/run_tests.sh           # everything
  bash scripts/run_tests.sh once      # exactly-once delivery only
  bash scripts/run_tests.sh dlq       # dead-letter queue only
  bash scripts/run_tests.sh retry     # backoff/retry only
  bash scripts/run_tests.sh reaper    # stale-reaper only
  bash scripts/run_tests.sh coverage  # everything + HTML coverage report
  ```
- **`scripts/db_inspect.py`** — a live terminal view of queue state, schema,
  indexes, and active locks. Run it with `--watch` while jobs are flowing
  through the system to see claims and heartbeats happen in real time.
- **`scripts/test_api.py`** — exercises every endpoint over HTTP with
  PASS/FAIL output, no pytest required — handy for a quick smoke test
  against a running deployment.
- **`benchmark/load.py`** — raw throughput/latency benchmark using the
  `noop` handler (configurable job count, concurrency, worker count).
- **`benchmark/math_benchmark.py`** — the correctness-and-performance
  benchmark described below.

---

## Test suite

```
$ docker compose exec api pytest tests/ -v
============================= test session starts ==============================
platform linux -- Python 3.12.13, pytest-8.2.2, pluggy-1.6.0
plugins: anyio-4.14.1, asyncio-0.24.0
collected 35 items

tests/test_dlq.py::test_dlq_after_exhausted_retries                     PASSED
tests/test_dlq.py::test_dlq_transition_is_atomic                        PASSED
tests/test_dlq.py::test_dlq_no_duplicates                               PASSED
tests/test_dlq.py::test_dlq_preserves_payload                          PASSED
tests/test_enqueue.py::test_enqueue_minimal_body                        PASSED
tests/test_enqueue.py::test_enqueue_full_body                           PASSED
tests/test_enqueue.py::test_enqueue_invalid_priority                    PASSED
tests/test_enqueue.py::test_enqueue_invalid_max_retries                 PASSED
tests/test_enqueue.py::test_get_job_found                               PASSED
tests/test_enqueue.py::test_get_job_not_found                           PASSED
tests/test_enqueue.py::test_list_jobs_empty                             PASSED
tests/test_enqueue.py::test_list_jobs_status_filter                     PASSED
tests/test_enqueue.py::test_list_jobs_pagination                        PASSED
tests/test_enqueue.py::test_list_jobs_invalid_status                    PASSED
tests/test_enqueue.py::test_cancel_pending_job                          PASSED
tests/test_enqueue.py::test_cancel_running_job_returns_409               PASSED
tests/test_enqueue.py::test_cancel_nonexistent_job                      PASSED
tests/test_enqueue.py::test_metrics_endpoint                            PASSED
tests/test_exactly_once.py::test_exactly_once_single_job                PASSED
tests/test_exactly_once.py::test_exactly_once_many_jobs                 PASSED
tests/test_exactly_once.py::test_advisory_lock_prevents_double_claim     PASSED
tests/test_priority.py::test_priority_order_single_worker                PASSED
tests/test_priority.py::test_try_claim_returns_highest_priority          PASSED
tests/test_priority.py::test_priority_tiebreak_by_run_at                 PASSED
tests/test_priority.py::test_future_scheduled_job_not_claimed             PASSED
tests/test_retry.py::test_retry_sets_run_at_to_future                    PASSED
tests/test_retry.py::test_full_retry_cycle_flaky_job                     PASSED
tests/test_retry.py::test_attempt_increments_on_each_failure              PASSED
tests/test_retry.py::test_backoff_delay_formula                          PASSED
tests/test_stale_reaper.py::test_stale_running_job_reclaimed              PASSED
tests/test_stale_reaper.py::test_stale_claimed_job_reclaimed              PASSED
tests/test_stale_reaper.py::test_terminal_jobs_not_reclaimed              PASSED
tests/test_stale_reaper.py::test_fresh_running_job_not_reclaimed          PASSED
tests/test_stale_reaper.py::test_end_to_end_reaper_then_worker            PASSED
tests/test_stale_reaper.py::test_reaper_loop_shuts_down_cleanly           PASSED

======================= 35 passed, 2 warnings in 44.64s ========================
```

Every test runs against a real PostgreSQL instance (`taskqueue_test`) — there
is no mocking of the database layer. `tests/conftest.py` creates the schema
once per session but a fresh connection pool *per test function*, which
matters under `pytest-asyncio`: a session-scoped pool bound to one event
loop will throw "Future attached to a different loop" the moment a
function-scoped test tries to use it.

```bash
# Run it yourself
docker compose up -d postgres
pip install -r requirements.txt
pytest tests/ -v
```

---

## Benchmarks

Most queue benchmarks measure speed. This one measures speed *and*
correctness, by running jobs whose results can be independently recomputed
and checked — a trial-division primality test, a Collatz (3n+1) sequence
walk, and a chained SHA-256 hash. If even one result comes back wrong, the
benchmark says so.

```
┌─────────────────────────────────────────────────────────┐
│            Math Benchmark — Verified Results             │
├─────────────────────────────────────────────────────────┤
│  Jobs submitted               600                        │
│  Jobs completed (done)        600                        │
│  Jobs failed/timed out        0                          │
├─────────────────────────────────────────────────────────┤
│  ✓ Correct results            600/600                    │
│  ✗ Incorrect results          0                          │
│  Accuracy                     100.0000%                  │
├─────────────────────────────────────────────────────────┤
│  End-to-end throughput        126 jobs/s                 │
│  p50 latency                  704.7 ms                   │
│  p75 latency                  942.8 ms                   │
│  p95 latency                  1096.0 ms                  │
│  p99 latency                  1131.7 ms                   │
│  max latency                  1162.6 ms                   │
├─────────────────────────────────────────────────────────┤
│                  Per-type breakdown                       │
├─────────────────────────────────────────────────────────┤
│  sha256_chain    200/200  correct  p50=674.2ms  p99=1150.3ms │
│  collatz         200/200  correct  p50=716.3ms  p99=1131.7ms │
│  is_prime        200/200  correct  p50=709.4ms  p99=1143.7ms │
└─────────────────────────────────────────────────────────┘

✓ ALL RESULTS MATHEMATICALLY CORRECT
```

Run on a 2023 MacBook Pro M2, Docker Desktop, default (untuned)
PostgreSQL 16, 4 worker goroutines.

```bash
docker compose up -d
docker compose exec api python benchmark/math_benchmark.py --jobs 600
```

For a raw throughput/latency number under your own concurrency and worker
count (independent of correctness checking), use `benchmark/load.py`:

```bash
docker compose up -d
python benchmark/load.py --jobs 10000 --concurrency 50 --workers 4
```

Exact numbers will depend on your hardware and Postgres tuning — that's the
honest reason this README headlines the verified run above rather than a
single throughput figure.

---

## Known limitations

**`LISTEN/NOTIFY` delivery isn't guaranteed.** `pg_notify` is best-effort —
a notification fired while a worker is restarting is simply dropped. The
5-second polling fallback bounds the damage, but it does mean worst-case
dispatch latency is 5 seconds, not near-zero. A dedicated broker (Kafka,
SQS) would persist notifications to disk instead.

**A single PostgreSQL host is the eventual bottleneck.** Every enqueue,
claim, heartbeat, and result write hits the same instance. Around 5,000
enqueues/second, WAL and connection overhead become the ceiling. Scaling
horizontally means partitioning `jobs` by `hash(id)` and pinning workers to
partitions — non-trivial, since advisory locks are global rather than
partition-scoped.

**PgBouncer in transaction-pooling mode breaks advisory locks.** Advisory
locks are connection-scoped. Under transaction pooling, a
`pg_try_advisory_lock` call and its matching `pg_advisory_unlock` can land on
different backend connections, and the unlock will silently no-op, leaking
the lock. Use PgBouncer in session-pooling mode, or skip it entirely and
rely on asyncpg's own pool, which holds connections open and preserves
session-scoped state — which is exactly what this project does.

**No admission control.** `POST /jobs` succeeds immediately regardless of
queue depth. Under extreme backlog (millions of pending jobs), the partial
index on `status='pending'` degrades and claim latency rises. An
admission-control layer (reject enqueues above N pending) plus table
partitioning would address this.

**Heartbeat window vs. stale threshold is a real trade-off.** The default
`STALE_THRESHOLD` (30s) is two heartbeat cycles (2 × 10s) plus a 10s safety
margin. A GC pause or an overloaded host can still cause a healthy worker to
miss its window and have its job reclaimed. If the original worker then
*also* finishes the job, you get a last-writer-wins double-write to the
result column — harmless for `noop`-style jobs, but worth auditing for any
handler with real side effects.

---

## What I'd do differently at scale

1. **A dedicated broker for fan-out.** Swap `LISTEN/NOTIFY` for Kafka or SQS
   to get guaranteed delivery, consumer-group semantics, and horizontal
   topic partitioning — Postgres stays the source of truth for job state,
   the broker just handles routing.
2. **Partition the `jobs` table.** `PARTITION BY HASH(id)` across 8–16
   child tables lets workers pin to specific partitions, shrinking lock
   contention and keeping each partition's partial index small and fast.
3. **A read replica for status polling.** `GET /jobs/{id}` and `GET /jobs`
   don't need the primary at all — routing them to a replica halves the
   primary's read load and keeps it focused on writes and advisory locks.
4. **Separate the claim connection from the execution connection.** Today,
   the advisory lock is acquired and held on a dedicated connection for the
   life of the job — correct, but it ties up that connection for the
   duration of long-running handlers. A connection reserved purely for
   claim/lock bookkeeping, decoupled from execution, would free that up.
5. **A Prometheus + Grafana stack in front of `/metrics/prometheus`.**
   Scrape every 15s; alert on `taskqueue_queue_depth{status="pending"} >
   10000` and `taskqueue_execution_seconds{quantile="0.99"} > 30`.

---

## Tech stack

| Layer | Choice |
|---|---|
| API | FastAPI 0.111, served by Uvicorn (uvloop + httptools) |
| Database driver | asyncpg 0.29 — no ORM, raw SQL for full control |
| Validation | Pydantic v2 |
| Metrics | `prometheus-client` |
| Database | PostgreSQL 16 (alpine image) |
| Tests | pytest + pytest-asyncio + httpx, against a real Postgres instance |
| Containers | Python 3.12-slim, non-root user, healthchecks on both services |

## Project layout

```
api/            FastAPI app, routes (jobs, metrics), Pydantic schemas
worker/         Worker loop, heartbeat/reaper, handler registry, shutdown signal
db/             asyncpg pool factory, SQL migration (schema + trigger + indexes)
benchmark/      Throughput benchmark (load.py) and correctness benchmark (math_benchmark.py)
scripts/        Test runner, live DB inspector, no-pytest API test runner
tests/          35 integration tests, fixtures for a real Postgres test database
Dockerfile.api / Dockerfile.worker / docker-compose.yml
```

---
