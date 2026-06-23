-- =============================================================================
-- Distributed Task Queue — Initial Schema
-- =============================================================================
-- Design notes:
--   • BIGSERIAL for job IDs because pg_try_advisory_lock() takes a BIGINT.
--   • status uses a CHECK constraint — no enum type so migrations stay simpler.
--   • Partial indexes keep the hot-path index small: only 'pending' rows compete
--     for workers; only 'claimed' rows are checked by the reaper.
-- =============================================================================

CREATE TABLE jobs (
  id            BIGSERIAL PRIMARY KEY,
  type          TEXT NOT NULL,
  payload       JSONB NOT NULL DEFAULT '{}',
  status        TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','claimed','running','done','failed','dead')),
  priority      INT  NOT NULL DEFAULT 5
                CHECK (priority BETWEEN 1 AND 10),
  attempt       INT  NOT NULL DEFAULT 0,
  max_retries   INT  NOT NULL DEFAULT 3,
  run_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claimed_at    TIMESTAMPTZ,
  heartbeat_at  TIMESTAMPTZ,
  completed_at  TIMESTAMPTZ,
  result        JSONB,
  error         TEXT,
  worker_id     TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partial index: only rows workers actually compete for.
-- Covering priority DESC, run_at ASC matches the ORDER BY in the claim query
-- so Postgres can satisfy ORDER BY + LIMIT directly from the index without
-- a heap sort.
CREATE INDEX idx_jobs_queue
  ON jobs (priority DESC, run_at ASC)
  WHERE status = 'pending';

-- Partial index for the stale-reaper: filters on heartbeat_at only among
-- rows that are actively held by a worker.
CREATE INDEX idx_jobs_claimed
  ON jobs (heartbeat_at)
  WHERE status IN ('claimed', 'running');

-- Status + created_at index for the LIST /jobs endpoint filters.
CREATE INDEX idx_jobs_status_created
  ON jobs (status, created_at DESC);

-- Type index for filtering by job type on the list endpoint.
CREATE INDEX idx_jobs_type
  ON jobs (type);

-- =============================================================================
-- Dead-letter table
-- =============================================================================
-- Jobs that exhaust max_retries land here with their full context so an
-- operator can inspect, replay, or discard them manually.
CREATE TABLE dead_letter_jobs (
  id            BIGSERIAL PRIMARY KEY,
  original_id   BIGINT  NOT NULL,
  type          TEXT    NOT NULL,
  payload       JSONB   NOT NULL,
  error         TEXT,
  attempts      INT,
  worker_id     TEXT,
  died_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dlq_original ON dead_letter_jobs (original_id);
CREATE INDEX idx_dlq_died_at  ON dead_letter_jobs (died_at DESC);

-- =============================================================================
-- LISTEN/NOTIFY trigger
-- =============================================================================
-- Fires pg_notify('job_channel', job_id) on every INSERT into jobs.
-- Workers holding a LISTEN connection wake up immediately rather than
-- waiting for the 5-second polling fallback.
-- The trigger fires AFTER INSERT so the row is already visible to workers
-- when they receive the notification (no phantom-read window).
CREATE OR REPLACE FUNCTION notify_new_job()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('job_channel', NEW.id::text);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_new_job
  AFTER INSERT ON jobs
  FOR EACH ROW EXECUTE FUNCTION notify_new_job();

-- =============================================================================
-- Metrics helper view (used by GET /metrics)
-- =============================================================================
CREATE OR REPLACE VIEW job_queue_stats AS
SELECT
  status,
  COUNT(*)                                    AS count,
  AVG(EXTRACT(EPOCH FROM (completed_at - created_at)))
    FILTER (WHERE status = 'done')            AS avg_execution_seconds
FROM jobs
GROUP BY status;
