"""
tests/conftest.py
-----------------
pytest fixtures for integration tests against a real PostgreSQL database.

No mocking — every test hits a real Postgres instance (taskqueue_test).

Key fixtures
------------
db_setup      session-scoped — creates the test DB + applies schema once.
               Also sets DATABASE_URL env var so workers auto-target the test DB.
pool          SESSION-scoped asyncpg.Pool — created once, shared across all tests.
clean_tables  autouse function-scoped — truncates all tables before each test.
client        SESSION-scoped httpx.AsyncClient wired to a FastAPI app.
enqueue       helper coroutine: inserts a job and returns its id (int).
test_dsn      convenience fixture exposing the test DB DSN string.

Fix applied (vs original):
  pool   — added scope="session"  (was function-scoped by default → InterfaceError)
  client — added scope="session"  (was function-scoped by default → InterfaceError)
  TRUNCATE moved from pool body → clean_tables autouse fixture so it still
  runs before every test without recreating the pool each time.

Environment variables (defaults work with docker-compose):
  TEST_DB_URL   — explicit DSN for the test database (highest priority)
  DATABASE_URL  — if set, replaces /taskqueue with /taskqueue_test

Compatibility: pytest-asyncio >= 0.21 (loop_scope API).
"""

from __future__ import annotations

import json
import os
import pathlib

import asyncpg
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

# ---------------------------------------------------------------------------
# Test DB DSN
# ---------------------------------------------------------------------------

_DEFAULT_TEST_DSN = "postgresql://taskuser:taskpass@localhost:5432/taskqueue_test"

SCHEMA_PATH = (
    pathlib.Path(__file__).parent.parent / "db" / "migrations" / "001_init.sql"
)


def _test_dsn() -> str:
    explicit = os.environ.get("TEST_DB_URL")
    if explicit:
        return explicit
    base = os.environ.get("DATABASE_URL", _DEFAULT_TEST_DSN)
    if base.endswith("/taskqueue"):
        return base[: -len("/taskqueue")] + "/taskqueue_test"
    if "/taskqueue_test" not in base:
        return base  # already a custom DSN, use as-is
    return base


# ---------------------------------------------------------------------------
# Session-scoped DB setup — runs once for the entire test session
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def db_setup() -> str:
    """
    Create the test database, apply the schema, and return the DSN.

    Also sets DATABASE_URL in the process environment so that any code
    that calls _db_dsn() (e.g. run_worker's dedicated connections) picks
    up the test database automatically.
    """
    dsn       = _test_dsn()
    db_name   = dsn.rsplit("/", 1)[-1]
    admin_dsn = dsn.rsplit("/", 1)[0] + "/postgres"

    # Create (or re-create) the test database
    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            db_name,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        await admin.execute(f'CREATE DATABASE "{db_name}"')
    finally:
        await admin.close()

    # Apply schema
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(SCHEMA_PATH.read_text())
    finally:
        await conn.close()

    # Point DATABASE_URL at the test DB so run_worker's dedicated connections
    # (which read _db_dsn()) target the right database.
    os.environ["DATABASE_URL"] = dsn

    yield dsn

    # Teardown — drop the test DB after the session ends
    os.environ.pop("DATABASE_URL", None)
    admin = await asyncpg.connect(admin_dsn)
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()",
            db_name,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
    finally:
        await admin.close()


# ---------------------------------------------------------------------------
# test_dsn convenience fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def test_dsn(db_setup: str) -> str:
    """Expose the test DB DSN for tests that need to pass it to run_worker."""
    return db_setup


# ---------------------------------------------------------------------------
# Session-scoped pool — created ONCE, shared across all tests
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def pool(db_setup: str) -> asyncpg.Pool:
    """
    asyncpg pool connected to the test DB.

    scope="session" is critical: creating and closing a pool per test on a
    shared session-scoped event loop causes asyncpg to raise:
      InterfaceError: cannot perform operation: another operation is in progress
    because teardown of test N races with setup of test N+1 on the same loop.

    Table truncation is handled by the separate clean_tables autouse fixture
    so every test still starts with an empty queue.
    """
    async def _init(conn: asyncpg.Connection) -> None:
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )
        await conn.set_type_codec(
            "json",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )

    p = await asyncpg.create_pool(
        dsn=db_setup,
        min_size=2,
        max_size=20,  # tests spin up many concurrent workers
        init=_init,
    )

    yield p

    await p.close()


# ---------------------------------------------------------------------------
# Per-test table reset — autouse so every test starts with an empty queue
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def clean_tables(pool: asyncpg.Pool) -> None:
    """
    Truncate all tables and reset BIGSERIAL counters before each test.
    RESTART IDENTITY ensures id=1 is always the first job in a fresh test,
    making assertion messages predictable.

    autouse=True means this runs automatically for every test without needing
    to be declared as a parameter.
    """
    await pool.execute(
        "TRUNCATE jobs, dead_letter_jobs RESTART IDENTITY CASCADE"
    )
    yield


# ---------------------------------------------------------------------------
# Session-scoped FastAPI test client
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def client(pool: asyncpg.Pool):
    """
    httpx.AsyncClient pointed at the FastAPI app.

    Injects the test pool directly into app.state, bypassing the lifespan
    create_pool() call so tests don't need a separate pool.

    scope="session" matches the pool scope — sharing one client for all tests
    avoids the same InterfaceError that affected the pool.
    """
    from api.main import create_app

    app = create_app()
    app.state.pool = pool

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Enqueue helper
# ---------------------------------------------------------------------------

@pytest.fixture()
def enqueue(pool: asyncpg.Pool):
    """
    Returns an async callable: enqueue(type, payload, priority, max_retries, run_at)
    Inserts a job directly into the DB and returns its id (int).
    """
    async def _enqueue(
        type: str = "noop",
        payload: dict | None = None,
        priority: int = 5,
        max_retries: int = 3,
        run_at=None,
    ) -> int:
        row = await pool.fetchrow(
            """
            INSERT INTO jobs (type, payload, priority, max_retries, run_at)
            VALUES ($1, $2, $3, $4, COALESCE($5, NOW()))
            RETURNING id
            """,
            type,
            payload or {},
            priority,
            max_retries,
            run_at,
        )
        return row["id"]

    return _enqueue