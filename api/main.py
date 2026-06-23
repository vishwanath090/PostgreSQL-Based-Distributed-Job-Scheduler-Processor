"""
api/main.py
-----------
FastAPI application factory.

The asyncpg pool is created once at startup and stored in app.state.pool so
every request handler can access it via the _get_pool() dependency without
managing connection lifetimes manually.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import jobs, metrics
from db.pool import close_pool, create_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan: pool open → serve → pool close
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API starting up — creating database pool …")
    app.state.pool = await create_pool()
    yield
    logger.info("API shutting down — closing database pool …")
    await close_pool(app.state.pool)


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="Distributed Task Queue",
        description=(
            "Production-grade job queue backed entirely by PostgreSQL. "
            "No Redis, no external broker — SKIP LOCKED + advisory locks + "
            "LISTEN/NOTIFY for exactly-once, push-driven delivery."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(jobs.router)
    app.include_router(metrics.router)

    @app.get("/health", tags=["ops"])
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
