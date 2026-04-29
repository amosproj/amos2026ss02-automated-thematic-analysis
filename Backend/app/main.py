from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from loguru import logger

from app.config import Settings, get_settings
from app.database import dispose_engine
from app.exceptions import register_exception_handlers
from app.logging_config import configure_logging
from app.middleware import register_middleware
from app.routers import register_routers


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings: Settings = get_settings()
    configure_logging(settings)
    logger.info("Starting — env={} debug={}", settings.APP_ENV, settings.APP_DEBUG)
    yield
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
    return app


app = create_app()
