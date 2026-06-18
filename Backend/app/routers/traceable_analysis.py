from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.dependencies import DbSession
from app.exceptions import NotFoundError, UnprocessableError
from app.models import TraceableAnalysisJob
from app.schemas.common import ResponseEnvelope
from app.schemas.traceable_analysis import (
    TraceableAnalysisJobCreateRequest,
    TraceableAnalysisJobSchema,
)
from app.services.traceable_analysis_jobs import traceable_analysis_job_runner

router = APIRouter(prefix="/codebooks", tags=["traceable-analysis"])


def _serialize_document_ids(document_ids: list[UUID] | None) -> str:
    if not document_ids:
        return "[]"
    return json.dumps([str(document_id) for document_id in document_ids])


def _deserialize_document_ids(document_ids_json: str) -> list[UUID]:
    raw_ids = json.loads(document_ids_json)
    return [UUID(raw_id) for raw_id in raw_ids]


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _compute_progress_percent(job: TraceableAnalysisJob) -> int:
    if job.status in {"succeeded", "failed", "cancelled"}:
        return 100
    if job.status == "queued":
        return 0
    phase_progress = {
        "extracting_quote_codes": 5,
        "consolidating_codes": 45,
        "synthesizing_themes": 65,
        "persisting_codebook": 80,
        "applying_codebook": 90,
    }
    if job.phase == "extracting_quote_codes" and job.analysis_units_total > 0:
        unit_progress = int((job.analysis_units_done * 35) / job.analysis_units_total)
        return max(5, min(40, 5 + unit_progress))
    return phase_progress.get(job.phase, 1)


def _to_job_schema(job: TraceableAnalysisJob) -> TraceableAnalysisJobSchema:
    return TraceableAnalysisJobSchema(
        id=job.id,
        status=job.status,
        phase=job.phase,
        progress_percent=_compute_progress_percent(job),
        codebook_name=job.codebook_name,
        analysis_name=job.analysis_name,
        custom_id=job.custom_id,
        corpus_id=job.corpus_id,
        transcript_document_ids=_deserialize_document_ids(job.transcript_document_ids_json),
        cancel_requested=job.cancel_requested,
        codebook_id=job.codebook_id,
        application_run_id=job.application_run_id,
        documents_total=job.documents_total,
        documents_done=job.documents_done,
        analysis_units_total=job.analysis_units_total,
        analysis_units_done=job.analysis_units_done,
        quotes_created=job.quotes_created,
        codes_created=job.codes_created,
        themes_created=job.themes_created,
        documents_coded=job.documents_coded,
        documents_failed=job.documents_failed,
        research_query=job.research_query,
        researcher_topics=job.researcher_topics,
        max_refinement_rounds=job.max_refinement_rounds,
        error_message=job.error_message,
        provenance_json=job.provenance_json,
        action_log_json=job.action_log_json,
        created_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
    )


@router.post(
    "/generate-apply-jobs",
    response_model=ResponseEnvelope[TraceableAnalysisJobSchema],
    status_code=202,
    summary="Create experimental traceable generation+application job",
)
async def create_traceable_analysis_job(
    payload: TraceableAnalysisJobCreateRequest,
    session: DbSession,
) -> JSONResponse:
    job = TraceableAnalysisJob(
        id=uuid4(),
        status="queued",
        phase="queued",
        codebook_name=payload.codebook_name,
        analysis_name=payload.analysis_name,
        custom_id=payload.custom_id,
        corpus_id=payload.corpus_id,
        transcript_document_ids_json=_serialize_document_ids(payload.transcript_document_ids),
        cancel_requested=False,
        documents_total=0,
        documents_done=0,
        analysis_units_total=0,
        analysis_units_done=0,
        research_query=payload.research_query,
        researcher_topics=payload.researcher_topics,
        max_refinement_rounds=payload.max_refinement_rounds,
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
    await traceable_analysis_job_runner.start()
    await traceable_analysis_job_runner.enqueue(job.id, session_factory=job_session_factory)
    return JSONResponse(
        status_code=202,
        content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"),
    )


@router.get(
    "/generate-apply-jobs/{job_id}",
    response_model=ResponseEnvelope[TraceableAnalysisJobSchema],
    summary="Get experimental traceable generation+application job",
)
async def get_traceable_analysis_job(
    job_id: UUID,
    session: DbSession,
) -> JSONResponse:
    job = await session.get(TraceableAnalysisJob, job_id)
    if job is None:
        raise NotFoundError(f"Traceable analysis job '{job_id}' not found")
    return JSONResponse(content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"))


@router.post(
    "/generate-apply-jobs/{job_id}/cancel",
    response_model=ResponseEnvelope[TraceableAnalysisJobSchema],
    status_code=202,
    summary="Cancel experimental traceable generation+application job",
)
async def cancel_traceable_analysis_job(
    job_id: UUID,
    session: DbSession,
) -> JSONResponse:
    job = await session.get(TraceableAnalysisJob, job_id)
    if job is None:
        raise NotFoundError(f"Traceable analysis job '{job_id}' not found")
    if job.status in {"succeeded", "failed", "cancelled"}:
        raise UnprocessableError(f"Job '{job_id}' is already finished with status '{job.status}'")

    job.cancel_requested = True
    if job.status == "queued":
        job.status = "cancelled"
        job.phase = "cancelled"
        job.finished_at = _utc_now_naive()
    await session.commit()
    await session.refresh(job)
    return JSONResponse(
        status_code=202,
        content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"),
    )
