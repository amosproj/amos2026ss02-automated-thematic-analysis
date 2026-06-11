from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.dependencies import DbSession
from app.exceptions import NotFoundError, UnprocessableError
from app.models import Codebook, CodebookGenerationJob
from app.schemas.codebook import (
    CodebookCreateRequest,
    CodebookDetailSchema,
    CodebookGenerateRequest,
    CodebookGenerationJobCreateRequest,
    CodebookGenerationJobSchema,
    CodebookSchema,
    GeneratedCodebookResponse,
    NodeInput,
)
from app.schemas.common import ResponseEnvelope
from app.services.codebook import CodebookService
from app.services.codebook_generation import CodebookGenerationService
from app.services.codebook_generation_jobs import codebook_generation_job_runner
from app.services.codebook_parser import parse_codebook_csv

router = APIRouter(prefix="/codebooks", tags=["codebooks"])


def _serialize_document_ids(document_ids: list[UUID] | None) -> str:
    if not document_ids:
        return "[]"
    return json.dumps([str(document_id) for document_id in document_ids])


def _deserialize_document_ids(document_ids_json: str) -> list[UUID]:
    raw_ids = json.loads(document_ids_json)
    return [UUID(raw_id) for raw_id in raw_ids]


def _to_job_schema(job: CodebookGenerationJob) -> CodebookGenerationJobSchema:
    phase = codebook_generation_job_runner.get_phase(job.id, status=job.status)
    progress_percent = _compute_job_progress_percent(job, phase=phase)
    return CodebookGenerationJobSchema(
        id=job.id,
        status=job.status,
        phase=phase,
        progress_percent=progress_percent,
        codebook_name=job.codebook_name,
        corpus_id=job.corpus_id,
        transcript_document_ids=_deserialize_document_ids(job.transcript_document_ids_json),
        cancel_requested=job.cancel_requested,
        research_query=job.research_query,
        researcher_topics=job.researcher_topics,
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


def _compute_job_progress_percent(job: CodebookGenerationJob, *, phase: str) -> int:
    if job.status in {"succeeded", "failed", "cancelled"}:
        return 100
    if job.status == "queued":
        return 0
    if phase == "consolidating":
        return 95
    if phase == "persisting":
        return 98
    if job.passages_total <= 0:
        return 1
    # Keep room for non-passage phases before terminal completion.
    passage_progress = int((job.passages_done * 90) / job.passages_total)
    return max(1, min(90, passage_progress))


@router.get(
    "/",
    response_model=ResponseEnvelope[list[CodebookSchema]],
    summary="List codebooks",
    description="Return all codebooks for a given corpus ordered by descending version.",
)
async def get_codebooks(
    corpus_id: UUID,
    session: DbSession,
) -> JSONResponse:
    stmt = select(Codebook).where(Codebook.corpus_id == corpus_id).order_by(desc(Codebook.version))
    codebooks = list((await session.scalars(stmt)).all())
    payload = [CodebookSchema.model_validate(codebook) for codebook in codebooks]
    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))


@router.post(
    "/generate",
    response_model=ResponseEnvelope[GeneratedCodebookResponse],
    status_code=201,
    summary="Generate codebook (synchronous)",
    description=(
        "Generate and persist a codebook immediately. "
        "If `transcript_document_ids` is provided, only those documents are used. "
        "If omitted or empty, all documents in the selected corpus are used."
    ),
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
        research_query=payload.research_query,
        researcher_topics=payload.researcher_topics,
    )
    return JSONResponse(
        status_code=201,
        content=ResponseEnvelope.ok(generated_codebook).model_dump(mode="json"),
    )


@router.post(
    "/generate-jobs",
    response_model=ResponseEnvelope[CodebookGenerationJobSchema],
    status_code=202,
    summary="Create codebook generation job",
    description=(
        "Create an asynchronous codebook generation job and return immediately. "
        "If `transcript_document_ids` is provided, only those documents are used. "
        "If omitted or empty, all documents in the selected corpus are used."
    ),
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
        research_query=payload.research_query,
        researcher_topics=payload.researcher_topics,
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
    codebook_generation_job_runner.set_phase(job.id, "queued")
    await codebook_generation_job_runner.enqueue(job.id, session_factory=job_session_factory)
    return JSONResponse(
        status_code=202,
        content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"),
    )


@router.get(
    "/generate-jobs",
    response_model=ResponseEnvelope[list[CodebookGenerationJobSchema]],
    summary="List codebook generation jobs",
    description=(
        "Return generation jobs for a corpus, newest first. Pass a "
        "comma-separated `status` filter (e.g. `queued,running`) to restrict "
        "the result; omit it to return every job for the corpus."
    ),
)
async def list_generate_codebook_jobs(
    corpus_id: UUID,
    session: DbSession,
    status: str | None = None,
) -> JSONResponse:
    stmt = select(CodebookGenerationJob).where(
        CodebookGenerationJob.corpus_id == corpus_id
    )
    if status:
        statuses = [s.strip() for s in status.split(",") if s.strip()]
        if statuses:
            stmt = stmt.where(CodebookGenerationJob.status.in_(statuses))
    stmt = stmt.order_by(desc(CodebookGenerationJob.created_at))
    jobs = list((await session.scalars(stmt)).all())
    payload = [_to_job_schema(job) for job in jobs]
    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))


@router.get(
    "/generate-jobs/{job_id}",
    response_model=ResponseEnvelope[CodebookGenerationJobSchema],
    summary="Get codebook generation job",
    description="Return the current status, progress, and result metadata of a generation job.",
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
    summary="Cancel codebook generation job",
    description=(
        "Request cancellation for a queued or running codebook generation job. "
        "Queued jobs are cancelled immediately; running jobs are cancelled when the worker observes the request."
    ),
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
        codebook_generation_job_runner.set_phase(job.id, "cancelled")
        job.finished_at = datetime.now(UTC).replace(tzinfo=None)
    await session.commit()
    await session.refresh(job)

    return JSONResponse(
        status_code=202,
        content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"),
    )


@router.post("/parse-csv", response_model=ResponseEnvelope[list[NodeInput]])
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
    codebook, themes, edges, codes, tc_edges = await service.create_codebook(payload)
    detail = CodebookService.build_detail_schema(codebook, themes, edges, codes, tc_edges)
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
    codebook, themes, edges, codes, theme_code_edges = await service.get_codebook_detail(codebook_id)
    detail = CodebookService.build_detail_schema(codebook, themes, edges, codes, theme_code_edges)
    return JSONResponse(content=ResponseEnvelope.ok(detail).model_dump(mode="json"))


@router.delete("/{codebook_id}", response_model=ResponseEnvelope[None])
async def delete_codebook(
    codebook_id: UUID,
    session: DbSession,
) -> JSONResponse:
    """Delete a codebook and all its themes/codes."""
    service = CodebookService(session)
    await service.delete_codebook(codebook_id)
    return JSONResponse(content=ResponseEnvelope.ok(None).model_dump(mode="json"))
