from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import desc, select

from app.dependencies import DbSession
from app.models import Codebook
from app.schemas.codebook import (
    CodebookGenerateRequest,
    CodebookSchema,
    GeneratedCodebookResponse,
)
from app.schemas.common import ResponseEnvelope
from app.services.codebook_generation import CodebookGenerationService

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


@router.post(
    "/generate",
    response_model=ResponseEnvelope[GeneratedCodebookResponse],
    status_code=201,
)
async def generate_codebook(
    payload: CodebookGenerateRequest,
    session: DbSession,
) -> JSONResponse:
    service = CodebookGenerationService(session)
    generated_codebook = await service.generate_codebook(
        codebook_name=payload.codebook_name,
        corpus_id=payload.corpus_id,
        transcript_document_ids=payload.transcript_document_ids,
    )
    return JSONResponse(
        status_code=201,
        content=ResponseEnvelope.ok(generated_codebook).model_dump(mode="json"),
    )
