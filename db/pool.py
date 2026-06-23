"""
db/pool.py
----------
asyncpg connection-pool factory.

One pool is shared by the FastAPI app; workers create their own pools so
their long-lived LISTEN connections don't starve the API under load.

Environment variables (all optional — defaults work with docker-compose):
  DATABASE_URL   full DSN, e.g. postgresql://user:pass@host:5432/dbname
  DB_MIN_SIZE    minimum connections in the pool  (default 2)
  DB_MAX_SIZE    maximum connections in the pool  (default 10)
  DB_CMD_TIMEOUT command timeout in seconds       (default 30)
"""

import asyncio
import json
import logging
import os
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DSN — matches the docker-compose service name and credentials.
# ---------------------------------------------------------------------------
_DEFAULT_DSN = "postgresql://taskuser:taskpass@postgres:5432/taskqueue"


def _dsn() -> str:
    return os.environ.get("DATABASE_URL", _DEFAULT_DSN)


def _min_size() -> int:
    return int(os.environ.get("DB_MIN_SIZE", "2"))


def _max_size() -> int:
    return int(os.environ.get("DB_MAX_SIZE", "10"))


def _cmd_timeout() -> float:
    return float(os.environ.get("DB_CMD_TIMEOUT", "30"))


# ---------------------------------------------------------------------------
# JSONB codec
# ---------------------------------------------------------------------------
# asyncpg returns JSONB columns as strings by default.  Registering a codec
# at the connection level converts them transparently to/from Python dicts.

async def _init_connection(conn: asyncpg.Connection) -> None:
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


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

async def create_pool(
    dsn: Optional[str] = None,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
) -> asyncpg.Pool:
    """Create and return a configured asyncpg connection pool.

    Retries with exponential back-off for up to 30 s so that the API /
    worker containers can start before Postgres is ready (common in compose).
    """
    resolved_dsn = dsn or _dsn()
    resolved_min = min_size if min_size is not None else _min_size()
    resolved_max = max_size if max_size is not None else _max_size()

    for attempt in range(6):
        try:
            pool = await asyncpg.create_pool(
                dsn=resolved_dsn,
                min_size=resolved_min,
                max_size=resolved_max,
                command_timeout=_cmd_timeout(),
                init=_init_connection,
            )
            logger.info(
                "Database pool created (min=%s, max=%s, dsn=%s)",
                resolved_min,
                resolved_max,
                resolved_dsn,
            )
            return pool
        except (asyncpg.PostgresConnectionError, OSError) as exc:
            wait = 2 ** attempt
            logger.warning(
                "Could not connect to Postgres (attempt %s/6): %s — retrying in %ss",
                attempt + 1,
                exc,
                wait,
            )
            if attempt == 5:
                raise
            await asyncio.sleep(wait)

    raise RuntimeError("Unreachable")


async def close_pool(pool: asyncpg.Pool) -> None:
    """Gracefully close all connections in the pool."""
    await pool.close()
    logger.info("Database pool closed")
