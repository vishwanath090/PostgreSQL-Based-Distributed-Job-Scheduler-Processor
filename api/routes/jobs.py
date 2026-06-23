"""
api/routes/jobs.py
------------------
POST   /jobs           — enqueue
GET    /jobs/{id}      — poll status
GET    /jobs           — list with filters
DELETE /jobs/{id}      — cancel pending job
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from api.schemas import JobCreate, JobListResponse, JobResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/jobs", tags=["jobs"])


# ---------------------------------------------------------------------------
# Dependency: pull the asyncpg pool out of app state
# ---------------------------------------------------------------------------

def _get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


# ---------------------------------------------------------------------------
# POST /jobs
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Enqueue a new job",
)
async def create_job(
    body: JobCreate,
    pool: asyncpg.Pool = Depends(_get_pool),
) -> JobResponse:
    """
    Insert a job into the queue.

    The database trigger fires pg_notify('job_channel', job_id) immediately
    after INSERT so workers listening on that channel wake up without waiting
    for the polling fallback.
    """
    try:
        row = await pool.fetchrow(
            """
            INSERT INTO jobs (type, payload, priority, max_retries, run_at)
            VALUES ($1, $2, $3, $4, COALESCE($5, NOW()))
            RETURNING *
            """,
            body.type,
            dict(body.payload),
            body.priority,
            body.max_retries,
            body.run_at,
        )
    except asyncpg.PostgresError as exc:
        logger.error("Failed to enqueue job: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to enqueue job") from exc

    return _row_to_response(row)


# ---------------------------------------------------------------------------
# GET /jobs/{id}
# ---------------------------------------------------------------------------

@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job status and result",
)
async def get_job(
    job_id: int,
    pool: asyncpg.Pool = Depends(_get_pool),
) -> JobResponse:
    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# GET /jobs  (list with filters)
# ---------------------------------------------------------------------------

@router.get(
    "",
    response_model=JobListResponse,
    summary="List jobs with optional filters",
)
async def list_jobs(
    pool: asyncpg.Pool = Depends(_get_pool),
    job_status: Optional[str] = Query(
        default=None,
        alias="status",
        description="Filter by status: pending|claimed|running|done|failed|dead",
    ),
    job_type: Optional[str] = Query(
        default=None,
        alias="type",
        description="Filter by job type",
    ),
    created_after: Optional[datetime] = Query(
        default=None,
        description="ISO-8601 timestamp — return jobs created after this time",
    ),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> JobListResponse:
    # Validate status value if provided
    valid_statuses = {"pending", "claimed", "running", "done", "failed", "dead"}
    if job_status and job_status not in valid_statuses:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status {job_status!r}. Must be one of {sorted(valid_statuses)}",
        )

    # Build WHERE clause dynamically
    conditions: list[str] = []
    params: list = []
    p = 1

    if job_status:
        conditions.append(f"status = ${p}")
        params.append(job_status)
        p += 1

    if job_type:
        conditions.append(f"type = ${p}")
        params.append(job_type)
        p += 1

    if created_after:
        conditions.append(f"created_at > ${p}")
        params.append(created_after)
        p += 1

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    count_query = f"SELECT COUNT(*) FROM jobs {where}"
    rows_query  = f"""
        SELECT * FROM jobs {where}
        ORDER BY created_at DESC
        LIMIT ${p} OFFSET ${p+1}
    """
    params_with_pagination = params + [limit, offset]

    total = await pool.fetchval(count_query, *params)
    rows  = await pool.fetch(rows_query, *params_with_pagination)

    return JobListResponse(
        items=[_row_to_response(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# DELETE /jobs/{id}
# ---------------------------------------------------------------------------

@router.delete(
    "/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
    summary="Cancel a pending job",
)
async def cancel_job(
    job_id: int,
    pool: asyncpg.Pool = Depends(_get_pool),
) -> None:
    """
    Cancel a job if it is still in 'pending' status.

    Jobs that are already claimed/running cannot be cancelled — the worker
    that holds the advisory lock is mid-execution.  Return 409 in that case.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT status FROM jobs WHERE id = $1 FOR UPDATE",
                job_id,
            )
            if row is None:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

            if row["status"] != "pending":
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Job {job_id} is in status '{row['status']}' and cannot be cancelled. "
                        "Only 'pending' jobs can be cancelled."
                    ),
                )

            await conn.execute(
                "DELETE FROM jobs WHERE id = $1",
                job_id,
            )


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _row_to_response(row: asyncpg.Record) -> JobResponse:
    return JobResponse(
        id=row["id"],
        type=row["type"],
        payload=row["payload"] or {},
        status=row["status"],
        priority=row["priority"],
        attempt=row["attempt"],
        max_retries=row["max_retries"],
        run_at=row["run_at"],
        claimed_at=row["claimed_at"],
        heartbeat_at=row["heartbeat_at"],
        completed_at=row["completed_at"],
        result=row["result"],
        error=row["error"],
        worker_id=row["worker_id"],
        created_at=row["created_at"],
    )