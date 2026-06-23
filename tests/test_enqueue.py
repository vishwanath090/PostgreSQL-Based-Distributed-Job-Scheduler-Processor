"""
tests/test_enqueue.py
---------------------
Tests for POST /jobs, GET /jobs/{id}, GET /jobs, and DELETE /jobs/{id}.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# POST /jobs
# ---------------------------------------------------------------------------

async def test_enqueue_minimal_body(client: AsyncClient):
    """POST /jobs with only required fields returns 201 with defaults."""
    resp = await client.post("/jobs", json={"type": "noop"})
    assert resp.status_code == 201
    data = resp.json()
    assert data["type"] == "noop"
    assert data["status"] == "pending"
    assert data["priority"] == 5
    assert data["attempt"] == 0
    assert data["max_retries"] == 3
    assert data["payload"] == {}
    assert "id" in data


async def test_enqueue_full_body(client: AsyncClient):
    """POST /jobs with all optional fields stores them correctly."""
    run_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    resp = await client.post("/jobs", json={
        "type":        "echo",
        "payload":     {"msg": "hello"},
        "priority":    9,
        "max_retries": 5,
        "run_at":      run_at,
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["priority"]    == 9
    assert data["max_retries"] == 5
    assert data["payload"]["msg"] == "hello"


async def test_enqueue_invalid_priority(client: AsyncClient):
    resp = await client.post("/jobs", json={"type": "noop", "priority": 99})
    assert resp.status_code == 422


async def test_enqueue_invalid_max_retries(client: AsyncClient):
    resp = await client.post("/jobs", json={"type": "noop", "max_retries": -1})
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /jobs/{id}
# ---------------------------------------------------------------------------

async def test_get_job_found(client: AsyncClient):
    create_resp = await client.post("/jobs", json={"type": "noop"})
    job_id = create_resp.json()["id"]

    resp = await client.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == job_id


async def test_get_job_not_found(client: AsyncClient):
    resp = await client.get("/jobs/999999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /jobs  (list)
# ---------------------------------------------------------------------------

async def test_list_jobs_empty(client: AsyncClient):
    resp = await client.get("/jobs")
    assert resp.status_code == 200
    assert resp.json()["total"] == 0
    assert resp.json()["items"] == []


async def test_list_jobs_status_filter(client: AsyncClient):
    # Enqueue 2 noop + 1 echo
    await client.post("/jobs", json={"type": "noop"})
    await client.post("/jobs", json={"type": "noop"})
    await client.post("/jobs", json={"type": "echo"})

    resp = await client.get("/jobs?status=pending&type=echo")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["items"][0]["type"] == "echo"


async def test_list_jobs_pagination(client: AsyncClient):
    for _ in range(5):
        await client.post("/jobs", json={"type": "noop"})

    resp = await client.get("/jobs?limit=2&offset=0")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["items"]) == 2
    assert data["total"] == 5
    assert data["limit"]  == 2
    assert data["offset"] == 0


async def test_list_jobs_invalid_status(client: AsyncClient):
    resp = await client.get("/jobs?status=bogus")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# DELETE /jobs/{id}
# ---------------------------------------------------------------------------

async def test_cancel_pending_job(client: AsyncClient, pool):
    create_resp = await client.post("/jobs", json={"type": "noop"})
    job_id = create_resp.json()["id"]

    resp = await client.delete(f"/jobs/{job_id}")
    assert resp.status_code == 204

    # Confirm deleted
    assert await pool.fetchrow("SELECT 1 FROM jobs WHERE id=$1", job_id) is None


async def test_cancel_running_job_returns_409(client: AsyncClient, pool):
    create_resp = await client.post("/jobs", json={"type": "noop"})
    job_id = create_resp.json()["id"]

    # Artificially mark as running
    await pool.execute(
        "UPDATE jobs SET status='running', heartbeat_at=NOW() WHERE id=$1", job_id
    )

    resp = await client.delete(f"/jobs/{job_id}")
    assert resp.status_code == 409


async def test_cancel_nonexistent_job(client: AsyncClient):
    resp = await client.delete("/jobs/999999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /metrics (smoke test)
# ---------------------------------------------------------------------------

async def test_metrics_endpoint(client: AsyncClient):
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    data = resp.json()
    assert "queue_depth" in data
    assert "jobs_per_second" in data
    assert "error_rate" in data
