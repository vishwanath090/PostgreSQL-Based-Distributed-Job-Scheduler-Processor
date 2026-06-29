"""
scripts/test_api.py
-------------------
Comprehensive API test runner — no pytest required.
Runs every endpoint, validates responses, shows PASS/FAIL with summary.

Usage (from project root):
    python scripts/test_api.py
    python scripts/test_api.py --url http://localhost:8000
    python scripts/test_api.py --verbose
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Coroutine

import httpx

# ---------------------------------------------------------------------------
# Colour helpers (no external deps)
# ---------------------------------------------------------------------------
_SUPPORTS_COLOR = sys.stdout.isatty() or "--color" in sys.argv

def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _SUPPORTS_COLOR else text

GREEN  = lambda t: _c("92", t)
RED    = lambda t: _c("91", t)
YELLOW = lambda t: _c("93", t)
CYAN   = lambda t: _c("96", t)
BOLD   = lambda t: _c("1",  t)
DIM    = lambda t: _c("2",  t)

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------
@dataclass
class Result:
    name: str
    passed: bool
    message: str = ""
    duration_ms: float = 0.0

_results: list[Result] = []

def _record(name: str, passed: bool, msg: str = "", ms: float = 0.0):
    _results.append(Result(name, passed, msg, ms))
    icon = GREEN("✓ PASS") if passed else RED("✗ FAIL")
    timing = DIM(f"  ({ms:.0f}ms)")
    print(f"  {icon}  {name}{timing}")
    if not passed and msg:
        print(f"         {YELLOW(msg)}")

# ---------------------------------------------------------------------------
# Test runner helper
# ---------------------------------------------------------------------------
async def _run(
    name: str,
    coro: Coroutine,
) -> Any:
    t0 = time.monotonic()
    try:
        result = await coro
        ms = (time.monotonic() - t0) * 1000
        _record(name, True, ms=ms)
        return result
    except AssertionError as e:
        ms = (time.monotonic() - t0) * 1000
        _record(name, False, str(e), ms)
        return None
    except Exception as e:
        ms = (time.monotonic() - t0) * 1000
        _record(name, False, f"{type(e).__name__}: {e}", ms)
        return None

# ---------------------------------------------------------------------------
# Individual test coroutines
# ---------------------------------------------------------------------------

async def t_health(c: httpx.AsyncClient):
    r = await c.get("/health")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    assert r.json()["status"] == "ok"


async def t_enqueue_minimal(c: httpx.AsyncClient) -> dict:
    r = await c.post("/jobs", json={"type": "noop"})
    assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"
    d = r.json()
    assert d["type"]        == "noop"
    assert d["status"]      == "pending"
    assert d["priority"]    == 5
    assert d["attempt"]     == 0
    assert d["max_retries"] == 3
    assert d["payload"]     == {}
    assert isinstance(d["id"], int)
    return d


async def t_enqueue_full(c: httpx.AsyncClient) -> dict:
    run_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    r = await c.post("/jobs", json={
        "type":        "echo",
        "payload":     {"msg": "hello", "count": 42},
        "priority":    9,
        "max_retries": 5,
        "run_at":      run_at,
    })
    assert r.status_code == 201, f"Expected 201, got {r.status_code}: {r.text}"
    d = r.json()
    assert d["priority"]        == 9
    assert d["max_retries"]     == 5
    assert d["payload"]["msg"]  == "hello"
    assert d["payload"]["count"]== 42
    assert d["status"]          == "pending"
    return d


async def t_enqueue_priority_boundary_low(c: httpx.AsyncClient):
    r = await c.post("/jobs", json={"type": "noop", "priority": 1})
    assert r.status_code == 201, f"priority=1 should be valid"
    assert r.json()["priority"] == 1


async def t_enqueue_priority_boundary_high(c: httpx.AsyncClient):
    r = await c.post("/jobs", json={"type": "noop", "priority": 10})
    assert r.status_code == 201, f"priority=10 should be valid"
    assert r.json()["priority"] == 10


async def t_enqueue_priority_invalid_zero(c: httpx.AsyncClient):
    r = await c.post("/jobs", json={"type": "noop", "priority": 0})
    assert r.status_code == 422, f"priority=0 must be rejected, got {r.status_code}"


async def t_enqueue_priority_invalid_eleven(c: httpx.AsyncClient):
    r = await c.post("/jobs", json={"type": "noop", "priority": 11})
    assert r.status_code == 422, f"priority=11 must be rejected, got {r.status_code}"


async def t_enqueue_no_type(c: httpx.AsyncClient):
    r = await c.post("/jobs", json={"payload": {"x": 1}})
    assert r.status_code == 422, f"Missing type must be rejected, got {r.status_code}"


async def t_enqueue_max_retries_boundary(c: httpx.AsyncClient):
    r = await c.post("/jobs", json={"type": "noop", "max_retries": 0})
    assert r.status_code == 201, f"max_retries=0 should be valid"
    r2 = await c.post("/jobs", json={"type": "noop", "max_retries": 10})
    assert r2.status_code == 201, f"max_retries=10 should be valid"


async def t_enqueue_scheduled(c: httpx.AsyncClient) -> dict:
    future = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    r = await c.post("/jobs", json={"type": "noop", "run_at": future})
    assert r.status_code == 201
    d = r.json()
    assert "run_at" in d
    # Scheduled job stays pending — worker won't claim it yet
    assert d["status"] == "pending"
    return d


async def t_enqueue_jsonb_payload(c: httpx.AsyncClient):
    r = await c.post("/jobs", json={
        "type": "echo",
        "payload": {
            "nested": {"deep": [1, 2, 3]},
            "unicode": "日本語",
            "flag": True,
            "null_val": None,
        }
    })
    assert r.status_code == 201
    d = r.json()
    assert d["payload"]["nested"]["deep"] == [1, 2, 3]
    assert d["payload"]["unicode"] == "日本語"
    assert d["payload"]["flag"] is True


async def t_get_job_by_id(c: httpx.AsyncClient, job_id: int):
    r = await c.get(f"/jobs/{job_id}")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    d = r.json()
    assert d["id"] == job_id
    assert "status" in d
    assert "created_at" in d


async def t_get_job_not_found(c: httpx.AsyncClient):
    r = await c.get("/jobs/999999999")
    assert r.status_code == 404, f"Expected 404, got {r.status_code}"


async def t_list_jobs_default(c: httpx.AsyncClient):
    r = await c.get("/jobs")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    d = r.json()
    assert "items"  in d
    assert "total"  in d
    assert "limit"  in d
    assert "offset" in d
    assert d["limit"]  == 50
    assert d["offset"] == 0
    assert isinstance(d["items"], list)


async def t_list_jobs_filter_by_status(c: httpx.AsyncClient):
    r = await c.get("/jobs", params={"status": "pending"})
    assert r.status_code == 200
    d = r.json()
    for item in d["items"]:
        assert item["status"] == "pending", f"Got non-pending item: {item['status']}"


async def t_list_jobs_filter_by_type(c: httpx.AsyncClient):
    r = await c.get("/jobs", params={"type": "echo"})
    assert r.status_code == 200
    d = r.json()
    for item in d["items"]:
        assert item["type"] == "echo", f"Got wrong type: {item['type']}"


async def t_list_jobs_invalid_status(c: httpx.AsyncClient):
    r = await c.get("/jobs", params={"status": "notavalidstatus"})
    assert r.status_code == 400, f"Expected 400 for invalid status, got {r.status_code}"


async def t_list_jobs_pagination(c: httpx.AsyncClient):
    # Enqueue 5 more jobs to ensure we have enough
    for _ in range(5):
        await c.post("/jobs", json={"type": "noop"})
    
    r1 = await c.get("/jobs", params={"limit": 3, "offset": 0})
    assert r1.status_code == 200
    r2 = await c.get("/jobs", params={"limit": 3, "offset": 3})
    assert r2.status_code == 200
    
    d1 = r1.json()
    d2 = r2.json()
    
    assert len(d1["items"]) <= 3
    assert d1["limit"]  == 3
    assert d2["offset"] == 3
    
    # IDs in page 1 should not appear in page 2
    ids1 = {it["id"] for it in d1["items"]}
    ids2 = {it["id"] for it in d2["items"]}
    assert not ids1.intersection(ids2), "Pagination overlap detected"


async def t_list_jobs_created_after(c: httpx.AsyncClient):
    # Use a past timestamp — all jobs should be returned
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    r = await c.get("/jobs", params={"created_after": past})
    assert r.status_code == 200
    
    # Use a far-future timestamp — no jobs
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    r2 = await c.get("/jobs", params={"created_after": future})
    assert r2.status_code == 200
    assert r2.json()["total"] == 0, "Jobs created_after future should return 0"


async def t_cancel_pending_job(c: httpx.AsyncClient):
    # Enqueue a future job so it stays pending
    future = (datetime.now(timezone.utc) + timedelta(hours=99)).isoformat()
    r = await c.post("/jobs", json={"type": "noop", "run_at": future})
    assert r.status_code == 201
    job_id = r.json()["id"]
    
    # Cancel it
    r2 = await c.delete(f"/jobs/{job_id}")
    assert r2.status_code == 204, f"Expected 204, got {r2.status_code}: {r2.text}"
    
    # Verify it's gone
    r3 = await c.get(f"/jobs/{job_id}")
    assert r3.status_code == 404, "Cancelled job should return 404"


async def t_cancel_nonexistent_job(c: httpx.AsyncClient):
    r = await c.delete("/jobs/999999999")
    assert r.status_code == 404, f"Expected 404, got {r.status_code}"


async def t_get_metrics(c: httpx.AsyncClient):
    r = await c.get("/metrics")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    d = r.json()
    
    assert "queue_depth"         in d
    assert "active_workers"      in d
    assert "jobs_per_second"     in d
    assert "error_rate"          in d
    assert "total_jobs"          in d
    assert "dead_letter_count"   in d
    
    qd = d["queue_depth"]
    assert "pending" in qd
    assert "running" in qd
    assert "done"    in qd
    assert "dead"    in qd
    
    assert isinstance(d["jobs_per_second"], float)
    assert 0.0 <= d["error_rate"] <= 1.0, f"error_rate out of range: {d['error_rate']}"


async def t_get_metrics_prometheus(c: httpx.AsyncClient):
    r = await c.get("/metrics/prometheus")
    assert r.status_code == 200, f"Expected 200, got {r.status_code}"
    text = r.text
    assert "taskqueue_queue_depth" in text,  "Missing queue_depth metric"
    assert "taskqueue_active_workers" in text, "Missing active_workers metric"
    assert 'status="pending"' in text or "pending" in text


async def t_metrics_counts_increase(c: httpx.AsyncClient):
    r1 = await c.get("/metrics")
    total_before = r1.json()["total_jobs"]
    
    # Enqueue 3 more jobs
    for _ in range(3):
        await c.post("/jobs", json={"type": "noop"})
    
    r2 = await c.get("/metrics")
    total_after = r2.json()["total_jobs"]
    
    assert total_after >= total_before + 3, (
        f"total_jobs should increase by 3: {total_before} → {total_after}"
    )


async def t_all_valid_statuses_in_filter(c: httpx.AsyncClient):
    valid_statuses = ["pending", "claimed", "running", "done", "failed", "dead"]
    for s in valid_statuses:
        r = await c.get("/jobs", params={"status": s})
        assert r.status_code == 200, f"Status {s!r} should be valid filter, got {r.status_code}"


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

async def run_all(base_url: str, verbose: bool):
    print(f"\n{BOLD('Task Queue — API Test Suite')}")
    print(f"  Target: {CYAN(base_url)}\n")

    # --- Connectivity check first ---
    try:
        async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as probe:
            r = await probe.get("/health")
            if r.status_code != 200:
                print(RED(f"  ✗  /health returned {r.status_code} — is the API running?"))
                sys.exit(1)
    except Exception as e:
        print(RED(f"  ✗  Cannot reach {base_url} — {e}"))
        print(DIM("     Run:  docker compose up -d"))
        sys.exit(1)

    async with httpx.AsyncClient(base_url=base_url, timeout=15.0) as c:

        # ── Health ─────────────────────────────────────────────────────────
        print(BOLD("  [Health]"))
        await _run("GET /health returns 200 + ok", t_health(c))

        # ── POST /jobs ─────────────────────────────────────────────────────
        print(BOLD("\n  [POST /jobs — happy path]"))
        base_job = await _run("minimal body → 201 with defaults", t_enqueue_minimal(c))
        await _run("full body → 201, all fields stored", t_enqueue_full(c))
        await _run("priority=1 (boundary low) → 201", t_enqueue_priority_boundary_low(c))
        await _run("priority=10 (boundary high) → 201", t_enqueue_priority_boundary_high(c))
        await _run("future run_at → 201, status=pending", t_enqueue_scheduled(c))
        await _run("complex JSONB payload → 201, nested values preserved", t_enqueue_jsonb_payload(c))
        await _run("max_retries=0 and =10 (boundaries) → 201", t_enqueue_max_retries_boundary(c))

        print(BOLD("\n  [POST /jobs — validation errors]"))
        await _run("missing type → 422", t_enqueue_no_type(c))
        await _run("priority=0 (below min) → 422", t_enqueue_priority_invalid_zero(c))
        await _run("priority=11 (above max) → 422", t_enqueue_priority_invalid_eleven(c))

        # ── GET /jobs/{id} ─────────────────────────────────────────────────
        print(BOLD("\n  [GET /jobs/{id}]"))
        if base_job:
            await _run(f"GET /jobs/{base_job['id']} → 200 with correct id",
                       t_get_job_by_id(c, base_job["id"]))
        await _run("GET /jobs/999999999 → 404", t_get_job_not_found(c))

        # ── GET /jobs (list) ───────────────────────────────────────────────
        print(BOLD("\n  [GET /jobs — list & filters]"))
        await _run("default list → 200 with pagination fields", t_list_jobs_default(c))
        await _run("?status=pending → all items are pending", t_list_jobs_filter_by_status(c))
        await _run("?type=echo → all items are echo type",  t_list_jobs_filter_by_type(c))
        await _run("?status=bad → 400 validation error", t_list_jobs_invalid_status(c))
        await _run("pagination limit+offset → no overlap", t_list_jobs_pagination(c))
        await _run("?created_after=future → 0 results", t_list_jobs_created_after(c))
        await _run("all 6 valid status values accepted", t_all_valid_statuses_in_filter(c))

        # ── DELETE /jobs/{id} ──────────────────────────────────────────────
        print(BOLD("\n  [DELETE /jobs/{id}]"))
        await _run("cancel pending job → 204, then 404 on GET", t_cancel_pending_job(c))
        await _run("cancel non-existent job → 404", t_cancel_nonexistent_job(c))

        # ── Metrics ────────────────────────────────────────────────────────
        print(BOLD("\n  [GET /metrics]"))
        await _run("GET /metrics → 200, all fields present", t_get_metrics(c))
        await _run("GET /metrics/prometheus → 200, Prometheus text format", t_get_metrics_prometheus(c))
        await _run("total_jobs increases after enqueue", t_metrics_counts_increase(c))

    # ── Summary ────────────────────────────────────────────────────────────
    passed = sum(1 for r in _results if r.passed)
    failed = sum(1 for r in _results if not r.passed)
    total  = len(_results)
    avg_ms = sum(r.duration_ms for r in _results) / total if total else 0

    print(f"\n{'─'*52}")
    print(BOLD(f"  Results: {GREEN(str(passed))} passed, {RED(str(failed)) if failed else str(failed)} failed  ({total} total)")
          if failed else BOLD(f"  Results: {GREEN(str(passed))} passed, 0 failed  ({total} total)"))
    print(DIM(f"  Avg response time: {avg_ms:.0f}ms"))

    if failed:
        print(f"\n  {RED('Failed tests:')}")
        for r in _results:
            if not r.passed:
                print(f"    {RED('✗')} {r.name}")
                print(f"      {DIM(r.message)}")
        sys.exit(1)
    else:
        print(GREEN(f"\n  All {total} API tests passed ✓"))

    print()


def main():
    parser = argparse.ArgumentParser(description="Task Queue API test runner")
    parser.add_argument("--url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    asyncio.run(run_all(args.url, args.verbose))


if __name__ == "__main__":
    main()
