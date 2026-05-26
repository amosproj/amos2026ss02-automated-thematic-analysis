from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.dependencies import DbSession
from app.exceptions import NotFoundError, UnprocessableError
from app.models import Codebook, CodebookGenerationJob
from app.schemas.codebook import (
    CodebookGenerateRequest,
    CodebookGenerationJobCreateRequest,
    CodebookGenerationJobSchema,
    CodebookSchema,
    GeneratedCodebookResponse,
)
from app.schemas.common import ResponseEnvelope
from app.services.codebook_generation import CodebookGenerationService
from app.services.codebook_generation_jobs import codebook_generation_job_runner

router = APIRouter(prefix="/codebooks", tags=["codebooks"])


def _serialize_document_ids(document_ids: list[UUID]) -> str:
    return json.dumps([str(document_id) for document_id in document_ids])


def _deserialize_document_ids(document_ids_json: str) -> list[UUID]:
    raw_ids = json.loads(document_ids_json)
    return [UUID(raw_id) for raw_id in raw_ids]


def _to_job_schema(job: CodebookGenerationJob) -> CodebookGenerationJobSchema:
    return CodebookGenerationJobSchema(
        id=job.id,
        status=job.status,
        codebook_name=job.codebook_name,
        corpus_id=job.corpus_id,
        transcript_document_ids=_deserialize_document_ids(job.transcript_document_ids_json),
        cancel_requested=job.cancel_requested,
        codebook_id=job.codebook_id,
        passages_total=job.passages_total,
        passages_done=job.passages_done,
        transcripts_processed=job.transcripts_processed,
        passages_processed=job.passages_processed,
        themes_created=job.themes_created,
        codes_created=job.codes_created,
        error_message=job.error_message,
        created_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
    )


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


@router.post(
    "/generate-jobs",
    response_model=ResponseEnvelope[CodebookGenerationJobSchema],
    status_code=202,
)
async def create_generate_codebook_job(
    payload: CodebookGenerationJobCreateRequest,
    session: DbSession,
) -> JSONResponse:
    job = CodebookGenerationJob(
        id=uuid4(),
        status="queued",
        codebook_name=payload.codebook_name,
        corpus_id=payload.corpus_id,
        transcript_document_ids_json=_serialize_document_ids(payload.transcript_document_ids),
        cancel_requested=False,
        passages_total=0,
        passages_done=0,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)

    bind = session.bind
    if bind is None:
        raise UnprocessableError("Database bind is unavailable for job execution")
    job_session_factory = async_sessionmaker(
        bind=bind,
        expire_on_commit=False,
        class_=AsyncSession,
    )

    await codebook_generation_job_runner.start()
    await codebook_generation_job_runner.enqueue(job.id, session_factory=job_session_factory)
    return JSONResponse(
        status_code=202,
        content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"),
    )


@router.get(
    "/generate-jobs/{job_id}",
    response_model=ResponseEnvelope[CodebookGenerationJobSchema],
)
async def get_generate_codebook_job(
    job_id: UUID,
    session: DbSession,
) -> JSONResponse:
    job = await session.get(CodebookGenerationJob, job_id)
    if job is None:
        raise NotFoundError(f"Codebook generation job '{job_id}' not found")
    return JSONResponse(content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"))


@router.post(
    "/generate-jobs/{job_id}/cancel",
    response_model=ResponseEnvelope[CodebookGenerationJobSchema],
    status_code=202,
)
async def cancel_generate_codebook_job(
    job_id: UUID,
    session: DbSession,
) -> JSONResponse:
    job = await session.get(CodebookGenerationJob, job_id)
    if job is None:
        raise NotFoundError(f"Codebook generation job '{job_id}' not found")

    if job.status in {"succeeded", "failed", "cancelled"}:
        raise UnprocessableError(f"Job '{job_id}' is already finished with status '{job.status}'")

    job.cancel_requested = True
    if job.status == "queued":
        job.status = "cancelled"
        job.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
    await session.commit()
    await session.refresh(job)

    return JSONResponse(
        status_code=202,
        content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"),
    )
