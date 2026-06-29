"""
tests/conftest.py
-----------------
pytest fixtures for integration tests against a real PostgreSQL database.

No mocking — every test hits a real Postgres instance (taskqueue_test).

Key fixtures
------------
db_setup      session-scoped — creates the test DB + applies schema once.
               Closes all connections before yielding, so no asyncpg
               connections are left open on the session event loop.
pool          FUNCTION-scoped asyncpg.Pool — created fresh for every test
               in that test's own event loop. Avoids the "Future attached
               to a different loop" crash that session-scoped pools cause
               when test functions run in function-scoped event loops
               (pytest-asyncio 0.24 behaviour).
clean_tables  autouse function-scoped — truncates all tables before each test.
client        function-scoped httpx.AsyncClient wired to the FastAPI app.
enqueue       helper coroutine: inserts a job and returns its id (int).
test_dsn      convenience fixture exposing the test DB DSN string.

Root cause of the loop mismatch (for reference)
------------------------------------------------
asyncio_default_fixture_loop_scope only governs async FIXTURES, not async
test functions. Test functions always run in a function-scoped event loop.
A session-scoped asyncpg pool is bound to the session event loop, so any
test function that touches it gets "Future attached to a different loop".

Fix: make pool/client function-scoped so they are created inside each
test's own event loop. db_setup remains session-scoped but closes every
connection before yielding, leaving no asyncpg state tied to the session loop.

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
        return base
    return base


# ---------------------------------------------------------------------------
# Session-scoped DB setup — runs once for the entire test session
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def db_setup() -> str:
    """
    Create the test database, apply the schema, and return the DSN.

    All asyncpg connections opened here are explicitly closed before this
    fixture yields, so nothing from the session event loop bleeds into the
    function-scoped pools created per test.
    """
    dsn       = _test_dsn()
    db_name   = dsn.rsplit("/", 1)[-1]
    admin_dsn = dsn.rsplit("/", 1)[0] + "/postgres"

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
        await admin.close()   # <-- closed before yield

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(SCHEMA_PATH.read_text())
    finally:
        await conn.close()    # <-- closed before yield

    os.environ["DATABASE_URL"] = dsn

    yield dsn

    # Teardown
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
# Function-scoped pool — created fresh for each test in its own event loop
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture()
async def pool(db_setup: str) -> asyncpg.Pool:
    """
    asyncpg pool connected to the test DB.

    MUST be function-scoped (the default).  pytest-asyncio 0.24 runs test
    functions in function-scoped event loops.  A session-scoped pool is bound
    to the session event loop, so using it inside a test raises:
      RuntimeError: Future … attached to a different loop

    Creating a fresh pool per test avoids this completely.  The overhead is
    acceptable: pool creation takes ~20 ms and there are only 35 tests.
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
        min_size=1,
        max_size=10,
        init=_init,
    )

    yield p

    await p.close()


# ---------------------------------------------------------------------------
# Per-test table reset — autouse so every test starts with an empty queue
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def clean_tables(pool: asyncpg.Pool) -> None:
    """
    Truncate all tables and reset BIGSERIAL counters before each test.
    """
    await pool.execute(
        "TRUNCATE jobs, dead_letter_jobs RESTART IDENTITY CASCADE"
    )
    yield


# ---------------------------------------------------------------------------
# Function-scoped FastAPI test client
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture()
async def client(pool: asyncpg.Pool):
    """
    httpx.AsyncClient pointed at the FastAPI app.

    Function-scoped to match the pool: the app gets the per-test pool
    injected into app.state so all requests share the same connection pool
    and event loop as the test itself.
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