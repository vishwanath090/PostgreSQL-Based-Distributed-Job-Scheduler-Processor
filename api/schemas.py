"""
api/schemas.py
--------------
Pydantic v2 request/response models for the Task Queue REST API.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Job lifecycle statuses
# ---------------------------------------------------------------------------
JobStatus = Literal["pending", "claimed", "running", "done", "failed", "dead"]


# ---------------------------------------------------------------------------
# Enqueue request
# ---------------------------------------------------------------------------

class JobCreate(BaseModel):
    """Body for POST /jobs."""

    type: str = Field(..., description="Handler name registered in the worker registry")
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary JSON payload forwarded to the handler",
    )
    priority: int = Field(
        default=5,
        ge=1,
        le=10,
        description="1 = lowest, 10 = highest",
    )
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Number of times to retry before dead-lettering",
    )
    run_at: Optional[datetime] = Field(
        default=None,
        description="Earliest time the job may run (ISO-8601). Omit for immediate.",
    )


# ---------------------------------------------------------------------------
# Job response
# ---------------------------------------------------------------------------

class JobResponse(BaseModel):
    """Returned by GET /jobs/{id} and POST /jobs."""

    id: int
    type: str
    payload: Dict[str, Any]
    status: JobStatus
    priority: int
    attempt: int
    max_retries: int
    run_at: datetime
    claimed_at: Optional[datetime]
    heartbeat_at: Optional[datetime]
    completed_at: Optional[datetime]
    result: Optional[Dict[str, Any]]
    error: Optional[str]
    worker_id: Optional[str]
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Job list response
# ---------------------------------------------------------------------------

class JobListResponse(BaseModel):
    """Returned by GET /jobs."""

    items: List[JobResponse]
    total: int
    limit: int
    offset: int


# ---------------------------------------------------------------------------
# Dead-letter response
# ---------------------------------------------------------------------------

class DeadLetterJobResponse(BaseModel):
    """Returned when a dead-lettered job is inspected."""

    id: int
    original_id: int
    type: str
    payload: Dict[str, Any]
    error: Optional[str]
    attempts: int
    worker_id: Optional[str]
    died_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Metrics response
# ---------------------------------------------------------------------------

class QueueDepthByStatus(BaseModel):
    pending: int = 0
    claimed: int = 0
    running: int = 0
    done: int = 0
    failed: int = 0
    dead: int = 0


class MetricsResponse(BaseModel):
    queue_depth: QueueDepthByStatus
    active_workers: int
    jobs_per_second: float
    error_rate: float           # fraction of failed+dead / total completed
    avg_execution_seconds: Optional[float]
    total_jobs: int
    dead_letter_count: int
