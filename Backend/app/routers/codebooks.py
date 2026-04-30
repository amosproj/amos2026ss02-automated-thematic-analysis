from __future__ import annotations

from sqlalchemy import desc, select

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.dependencies import DbSession
from app.models import Codebook
from app.schemas.codebook import CodebookSchema
from app.schemas.common import ResponseEnvelope

router = APIRouter(prefix="/codebooks", tags=["codebooks"])


@router.get("/", response_model=ResponseEnvelope[list[CodebookSchema]])
async def get_codebooks(
    session: DbSession,
) -> JSONResponse:
    # TODO: Add explicit version-aware filtering/selection semantics and pagination.
    # TODO: Move the SQl Query into a Service
    # TODO: Add tests
    # This is just a quick implementation to get something working.
    # The user needs to be able to select a project_id in the frontend in order to load a themes tree
    # (e.g. project_id + optional version/latest) instead of returning all rows.
    stmt = select(Codebook).order_by(Codebook.project_id.asc(), desc(Codebook.version))
    codebooks = list((await session.scalars(stmt)).all())
    payload = [CodebookSchema.model_validate(codebook) for codebook in codebooks]
    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))
