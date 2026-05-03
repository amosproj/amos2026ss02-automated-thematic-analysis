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
    # TODO: Completely refactor / replace this endpoint. This is just a quick implementation to get some working data
    #  for the frontend.
    # The user needs to be able to select a project_id in the frontend in order to load a themes tree
    stmt = select(Codebook).order_by(Codebook.project_id.asc(), desc(Codebook.version))
    codebooks = list((await session.scalars(stmt)).all())
    payload = [CodebookSchema.model_validate(codebook) for codebook in codebooks]
    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))
