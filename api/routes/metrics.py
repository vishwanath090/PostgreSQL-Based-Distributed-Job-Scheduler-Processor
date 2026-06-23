"""
api/routes/metrics.py
---------------------
GET /metrics   — operational metrics for dashboards / alerting
GET /metrics/prometheus  — Prometheus text exposition format

Metrics collected:
  • queue_depth by status (live count from jobs table)
  • active_workers  (jobs with status='running' as a proxy)
  • jobs_per_second (done jobs in the last 60 s / 60)
  • error_rate      (failed+dead / (done+failed+dead))
  • avg_execution_seconds  (mean completed_at - created_at for done jobs)
  • total_jobs
  • dead_letter_count
"""

from __future__ import annotations

import time
import logging
from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, Request
from fastapi.responses import PlainTextResponse
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

from api.schemas import MetricsResponse, QueueDepthByStatus

logger = logging.getLogger(__name__)
router = APIRouter(tags=["metrics"])

# ---------------------------------------------------------------------------
# Prometheus registry (separate from the default global one so we can use
# this in tests without interference)
# ---------------------------------------------------------------------------
_prom_registry = CollectorRegistry()

prom_jobs_total = Counter(
    "taskqueue_jobs_total",
    "Total jobs processed by status",
    ["status"],
    registry=_prom_registry,
)
prom_queue_depth = Gauge(
    "taskqueue_queue_depth",
    "Current number of jobs by status",
    ["status"],
    registry=_prom_registry,
)
prom_exec_seconds = Histogram(
    "taskqueue_execution_seconds",
    "Job execution latency in seconds",
    buckets=[0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300],
    registry=_prom_registry,
)
prom_active_workers = Gauge(
    "taskqueue_active_workers",
    "Number of jobs currently in running state",
    registry=_prom_registry,
)


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------

def _get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


# ---------------------------------------------------------------------------
# GET /metrics  (JSON)
# ---------------------------------------------------------------------------

@router.get(
    "/metrics",
    response_model=MetricsResponse,
    summary="Queue depth, throughput, error rate and latency metrics",
)
async def get_metrics(pool: asyncpg.Pool = Depends(_get_pool)) -> MetricsResponse:
    # --- Queue depth by status ---
    depth_rows = await pool.fetch(
        "SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status"
    )
    depth_map = {r["status"]: r["cnt"] for r in depth_rows}
    queue_depth = QueueDepthByStatus(
        pending=depth_map.get("pending", 0),
        claimed=depth_map.get("claimed", 0),
        running=depth_map.get("running", 0),
        done=depth_map.get("done", 0),
        failed=depth_map.get("failed", 0),
        dead=depth_map.get("dead", 0),
    )

    # Update Prometheus gauges
    for s, v in depth_map.items():
        prom_queue_depth.labels(status=s).set(v)

    # --- Active workers (proxy: count running jobs) ---
    active_workers = depth_map.get("running", 0)
    prom_active_workers.set(active_workers)

    # --- Jobs per second (done in last 60 s) ---
    recent_done: int = await pool.fetchval(
        """
        SELECT COUNT(*)
        FROM   jobs
        WHERE  status       = 'done'
          AND  completed_at > NOW() - INTERVAL '60 seconds'
        """
    ) or 0
    jobs_per_second = round(recent_done / 60.0, 3)

    # --- Error rate ---
    total_terminal: int = await pool.fetchval(
        "SELECT COUNT(*) FROM jobs WHERE status IN ('done','failed','dead')"
    ) or 0
    total_errored: int = await pool.fetchval(
        "SELECT COUNT(*) FROM jobs WHERE status IN ('failed','dead')"
    ) or 0
    error_rate = round(total_errored / total_terminal, 4) if total_terminal else 0.0

    # --- Average execution time ---
    avg_exec: Optional[float] = await pool.fetchval(
        """
        SELECT AVG(EXTRACT(EPOCH FROM (completed_at - created_at)))
        FROM   jobs
        WHERE  status = 'done'
          AND  completed_at IS NOT NULL
        """
    )

    # --- Total and DLQ counts ---
    total_jobs: int = await pool.fetchval("SELECT COUNT(*) FROM jobs") or 0
    dlq_count: int  = await pool.fetchval("SELECT COUNT(*) FROM dead_letter_jobs") or 0

    return MetricsResponse(
        queue_depth=queue_depth,
        active_workers=active_workers,
        jobs_per_second=jobs_per_second,
        error_rate=error_rate,
        avg_execution_seconds=float(avg_exec) if avg_exec is not None else None,
        total_jobs=total_jobs,
        dead_letter_count=dlq_count,
    )


# ---------------------------------------------------------------------------
# GET /metrics/prometheus  (Prometheus text format)
# ---------------------------------------------------------------------------

@router.get(
    "/metrics/prometheus",
    response_class=PlainTextResponse,
    summary="Prometheus exposition format",
    include_in_schema=False,
)
async def prometheus_metrics(pool: asyncpg.Pool = Depends(_get_pool)) -> PlainTextResponse:
    """
    Scrape endpoint compatible with any Prometheus server.

    Calls get_metrics() internally to refresh the gauges before rendering.
    """
    # Refresh Prometheus gauges via the JSON endpoint logic
    depth_rows = await pool.fetch(
        "SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status"
    )
    for r in depth_rows:
        prom_queue_depth.labels(status=r["status"]).set(r["cnt"])

    active = await pool.fetchval(
        "SELECT COUNT(*) FROM jobs WHERE status = 'running'"
    ) or 0
    prom_active_workers.set(active)

    output = generate_latest(_prom_registry)
    return PlainTextResponse(output.decode(), media_type=CONTENT_TYPE_LATEST)
