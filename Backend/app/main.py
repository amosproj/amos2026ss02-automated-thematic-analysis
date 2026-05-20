import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from loguru import logger

from app.config import Settings, get_settings
from app.database import check_db_connection, dispose_engine, init_db
from app.exceptions import register_exception_handlers
from app.logging_config import configure_logging
from app.middleware import register_middleware
from app.routers import register_routers
from app.services.upload_cleanup import run_upload_cleanup_loop


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings: Settings = get_settings()
    configure_logging(settings)
    logger.info("Starting — env={} debug={}", settings.APP_ENV, settings.APP_DEBUG)
    db_ok = await check_db_connection()
    if not db_ok:
        raise RuntimeError("Database connection failed. Check DATABASE_URL and Postgres availability.")
    await init_db()
    uploads_dir = Path(settings.UPLOADS_DIR).resolve()
    uploads_dir.mkdir(parents=True, exist_ok=True)

    cleanup_task = asyncio.create_task(
        run_upload_cleanup_loop(
            uploads_dir=uploads_dir,
            max_age_seconds=settings.DEMOGRAPHIC_UPLOAD_TTL_SECONDS,
            interval_seconds=settings.UPLOAD_CLEANUP_INTERVAL_SECONDS,
        )
    )

    try:
        yield
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        logger.info("Shutting down")
        await dispose_engine()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Backend API",
        version="0.1.0",
        openapi_url="/openapi.json" if not settings.is_production else None,
        docs_url="/docs" if not settings.is_production else None,
        redoc_url="/redoc" if not settings.is_production else None,
        lifespan=lifespan,
    )
    register_middleware(app, settings)
    register_exception_handlers(app)
    register_routers(app, settings)
    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    return app


app = create_app()
