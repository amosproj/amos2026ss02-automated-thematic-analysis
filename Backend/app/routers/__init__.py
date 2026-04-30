from fastapi import FastAPI

from app.config import Settings
from app.routers import health, ingestion


def register_routers(app: FastAPI, settings: Settings) -> None:
    prefix = settings.API_V1_PREFIX
    app.include_router(health.router, prefix=prefix)
    app.include_router(ingestion.router, prefix=prefix)
