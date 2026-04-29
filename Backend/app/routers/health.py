from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app import __version__
from app.database import check_db_connection
from app.schemas.common import HealthResponse, ResponseEnvelope

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/", response_model=ResponseEnvelope[HealthResponse])
async def liveness() -> JSONResponse:
    payload = ResponseEnvelope.ok(
        HealthResponse(status="ok", database="up", version=__version__)
    )
    return JSONResponse(content=payload.model_dump())


@router.get("/ready", response_model=ResponseEnvelope[HealthResponse])
async def readiness() -> JSONResponse:
    db_up = await check_db_connection()
    payload = ResponseEnvelope.ok(
        HealthResponse(
            status="ok" if db_up else "degraded",
            database="up" if db_up else "down",
            version=__version__,
        )
    )
    return JSONResponse(content=payload.model_dump(), status_code=200 if db_up else 503)
