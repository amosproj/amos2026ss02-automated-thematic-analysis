from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy import desc, select

from app.dependencies import DbSession
from app.exceptions import UnprocessableError
from app.models import Codebook
from app.schemas.codebook import (
    CodebookCreateRequest,
    CodebookDetailSchema,
    CodebookSchema,
    ThemeInput,
)
from app.schemas.common import ResponseEnvelope
from app.services.codebook import CodebookService
from app.services.codebook_parser import parse_codebook_csv

router = APIRouter(prefix="/codebooks", tags=["codebooks"])


@router.get("/", response_model=ResponseEnvelope[list[CodebookSchema]])
async def get_codebooks(
    session: DbSession,
) -> JSONResponse:
    stmt = select(Codebook).order_by(Codebook.project_id.asc(), desc(Codebook.version))
    codebooks = list((await session.scalars(stmt)).all())
    payload = [CodebookSchema.model_validate(codebook) for codebook in codebooks]
    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))


@router.post("/parse-csv", response_model=ResponseEnvelope[list[ThemeInput]])
async def parse_csv(
    file: UploadFile = File(...),
) -> JSONResponse:
    """Parse and validate a researcher-uploaded codebook CSV without saving it.

    Returns the parsed list of ThemeInput preview items, or a 422 error.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise UnprocessableError("Only CSV files are supported for codebook upload.")

    try:
        content = await file.read()
        themes = parse_codebook_csv(content)
        payload = [t.model_dump() for t in themes]
        return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))
    except UnprocessableError as exc:
        raise exc
    except Exception as exc:
        raise UnprocessableError(f"Failed to parse CSV file: {exc}") from exc


@router.post("/", response_model=ResponseEnvelope[CodebookDetailSchema], status_code=201)
async def create_codebook(
    payload: CodebookCreateRequest,
    session: DbSession,
) -> JSONResponse:
    """Create a new codebook and persist its themes atomically in the database."""
    service = CodebookService(session)
    codebook, themes, edges = await service.create_codebook(payload)
    detail = CodebookService.build_detail_schema(codebook, themes, edges)
    return JSONResponse(
        status_code=201,
        content=ResponseEnvelope.ok(detail).model_dump(mode="json"),
    )


@router.get("/{codebook_id}", response_model=ResponseEnvelope[CodebookDetailSchema])
async def get_codebook_detail(
    codebook_id: UUID,
    session: DbSession,
) -> JSONResponse:
    """Fetch details of a specific codebook, including all associated themes."""
    service = CodebookService(session)
    codebook, themes, edges = await service.get_codebook_detail(codebook_id)
    detail = CodebookService.build_detail_schema(codebook, themes, edges)
    return JSONResponse(content=ResponseEnvelope.ok(detail).model_dump(mode="json"))

