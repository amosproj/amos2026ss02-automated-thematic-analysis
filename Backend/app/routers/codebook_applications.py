from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.dependencies import DbSession
from app.exceptions import NotFoundError, UnprocessableError
from app.models import (
    CodeAssignment,
    Codebook,
    CodebookApplicationJob,
    CodebookApplicationRun,
    Corpus,
    CorpusDocument,
    DocumentCoding,
    ThemeAssignment,
)
from app.schemas.codebook_application import (
    CodeAssignmentSchema,
    CodebookApplicationJobCreateRequest,
    CodebookApplicationJobSchema,
    CodebookApplicationRunDetailSchema,
    CodebookApplicationRunSchema,
    DocumentCodingSchema,
    ThemeAssignmentSchema,
)
from app.schemas.common import ResponseEnvelope
from app.services.codebook_application_jobs import codebook_application_job_runner

router = APIRouter(tags=["codebook-applications"])


def _serialize_document_ids(document_ids: list[UUID] | None) -> str:
    if not document_ids:
        return "[]"
    return json.dumps([str(document_id) for document_id in document_ids])


def _deserialize_document_ids(document_ids_json: str) -> list[UUID]:
    raw_ids = json.loads(document_ids_json)
    return [UUID(raw_id) for raw_id in raw_ids]


def _compute_progress_percent(job: CodebookApplicationJob) -> int:
    if job.status in {"succeeded", "failed", "cancelled"}:
        return 100
    if job.status == "queued":
        return 0
    if job.phase == "loading_codebook":
        return 1
    if job.phase == "persisting":
        return 98
    if job.documents_total <= 0:
        return 1
    document_progress = int((job.documents_done * 95) / job.documents_total)
    return max(1, min(95, document_progress))


def _to_job_schema(job: CodebookApplicationJob) -> CodebookApplicationJobSchema:
    return CodebookApplicationJobSchema(
        id=job.id,
        name=job.name,
        custom_id=job.custom_id,
        status=job.status,
        phase=job.phase,
        progress_percent=_compute_progress_percent(job),
        corpus_id=job.corpus_id,
        codebook_id=job.codebook_id,
        transcript_document_ids=_deserialize_document_ids(job.transcript_document_ids_json),
        cancel_requested=job.cancel_requested,
        application_run_id=job.application_run_id,
        documents_total=job.documents_total,
        documents_done=job.documents_done,
        documents_coded=job.documents_coded,
        documents_failed=job.documents_failed,
        error_message=job.error_message,
        created_at=job.created_at,
        updated_at=job.updated_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
    )


def _to_run_schema(run: CodebookApplicationRun, transcript_document_ids: list[UUID] | None = None) -> CodebookApplicationRunSchema:
    schema = CodebookApplicationRunSchema.model_validate(run)
    if transcript_document_ids is not None:
        schema.transcript_document_ids = transcript_document_ids
    return schema


def _to_document_coding_schema(
    document_coding: DocumentCoding,
    *,
    theme_assignments: list[ThemeAssignment],
    code_assignments: list[CodeAssignment],
) -> DocumentCodingSchema:
    return DocumentCodingSchema(
        id=document_coding.id,
        application_run_id=document_coding.application_run_id,
        document_id=document_coding.document_id,
        codebook_id=document_coding.codebook_id,
        status=document_coding.status,
        summary=document_coding.summary,
        researcher_notes=document_coding.researcher_notes,
        error_message=document_coding.error_message,
        created_at=document_coding.created_at,
        updated_at=document_coding.updated_at,
        theme_assignments=[
            ThemeAssignmentSchema.model_validate(assignment)
            for assignment in theme_assignments
        ],
        code_assignments=[
            CodeAssignmentSchema.model_validate(assignment)
            for assignment in code_assignments
        ],
    )


async def _validate_job_create_payload(
    *,
    codebook_id: UUID,
    payload: CodebookApplicationJobCreateRequest,
    session: DbSession,
) -> UUID:
    codebook = await session.get(Codebook, codebook_id)
    if codebook is None:
        raise NotFoundError(f"Codebook '{codebook_id}' not found")
    corpus_id = codebook.corpus_id
    if payload.corpus_id is not None and codebook.corpus_id != payload.corpus_id:
        raise UnprocessableError(
            f"Codebook '{codebook_id}' does not belong to corpus '{payload.corpus_id}'"
        )
    if not payload.transcript_document_ids:
        return corpus_id

    documents = list(
        (
            await session.scalars(
                select(CorpusDocument.id).where(
                    CorpusDocument.corpus_id == corpus_id,
                    CorpusDocument.id.in_(payload.transcript_document_ids),
                )
            )
        ).all()
    )
    found_ids = set(documents)
    missing = [document_id for document_id in payload.transcript_document_ids if document_id not in found_ids]
    if missing:
        missing_str = ", ".join(str(document_id) for document_id in missing)
        raise UnprocessableError(
            "Some transcript_document_ids were not found in the selected corpus: "
            f"{missing_str}"
        )
    return corpus_id


@router.post(
    "/codebooks/{codebook_id}/apply-jobs",
    response_model=ResponseEnvelope[CodebookApplicationJobSchema],
    status_code=202,
    summary="Create codebook application job",
)
async def create_apply_codebook_job(
    codebook_id: UUID,
    payload: CodebookApplicationJobCreateRequest,
    session: DbSession,
) -> JSONResponse:
    corpus_id = await _validate_job_create_payload(codebook_id=codebook_id, payload=payload, session=session)

    # Auto-generate name and custom_id if empty
    name = payload.name
    custom_id = payload.custom_id

    if not name or not custom_id:
        # Get run count for this corpus to generate incrementing ID
        run_count_query = select(func.count(CodebookApplicationRun.id)).where(CodebookApplicationRun.corpus_id == corpus_id)
        run_count = await session.scalar(run_count_query) or 0
        run_number = run_count + 1

        if not custom_id:
            custom_id = f"RUN-{run_number:03d}"

        if not name:
            corpus = await session.get(Corpus, corpus_id)
            corpus_name = corpus.name if corpus else "Corpus"
            if run_number == 1:
                name = f"{corpus_name} Analysis"
            else:
                name = f"{corpus_name} Analysis {run_number}"

    job = CodebookApplicationJob(
        id=uuid4(),
        name=name,
        custom_id=custom_id,
        status="queued",
        phase="queued",
        corpus_id=corpus_id,
        codebook_id=codebook_id,
        transcript_document_ids_json=_serialize_document_ids(payload.transcript_document_ids),
        cancel_requested=False,
        documents_total=0,
        documents_done=0,
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

    await codebook_application_job_runner.start()
    await codebook_application_job_runner.enqueue(job.id, session_factory=job_session_factory)
    return JSONResponse(
        status_code=202,
        content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"),
    )


@router.get(
    "/codebooks/apply-jobs/{job_id}",
    response_model=ResponseEnvelope[CodebookApplicationJobSchema],
    summary="Get codebook application job",
)
async def get_apply_codebook_job(
    job_id: UUID,
    session: DbSession,
) -> JSONResponse:
    job = await session.get(CodebookApplicationJob, job_id)
    if job is None:
        raise NotFoundError(f"Codebook application job '{job_id}' not found")
    return JSONResponse(content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"))


@router.post(
    "/codebooks/apply-jobs/{job_id}/cancel",
    response_model=ResponseEnvelope[CodebookApplicationJobSchema],
    status_code=202,
    summary="Cancel codebook application job",
)
async def cancel_apply_codebook_job(
    job_id: UUID,
    session: DbSession,
) -> JSONResponse:
    job = await session.get(CodebookApplicationJob, job_id)
    if job is None:
        raise NotFoundError(f"Codebook application job '{job_id}' not found")
    if job.status in {"succeeded", "failed", "cancelled"}:
        raise UnprocessableError(f"Job '{job_id}' is already finished with status '{job.status}'")

    job.cancel_requested = True
    if job.status == "queued":
        job.status = "cancelled"
        job.phase = "cancelled"
        job.finished_at = datetime.now(UTC).replace(tzinfo=None)
    await session.commit()
    await session.refresh(job)
    return JSONResponse(
        status_code=202,
        content=ResponseEnvelope.ok(_to_job_schema(job)).model_dump(mode="json"),
    )


@router.get(
    "/codebooks/{codebook_id}/application-runs",
    response_model=ResponseEnvelope[list[CodebookApplicationRunSchema]],
    summary="List codebook application runs",
)
async def list_codebook_application_runs(
    codebook_id: UUID,
    session: DbSession,
) -> JSONResponse:
    stmt = (
        select(CodebookApplicationRun)
        .where(CodebookApplicationRun.codebook_id == codebook_id)
        .order_by(desc(CodebookApplicationRun.created_at))
    )
    runs = list((await session.scalars(stmt)).all())

    if not runs:
        return JSONResponse(content=ResponseEnvelope.ok([]).model_dump(mode="json"))

    run_ids = [run.id for run in runs]

    docs_stmt = select(DocumentCoding.application_run_id, DocumentCoding.document_id).where(
        DocumentCoding.application_run_id.in_(run_ids)
    )
    docs_result = await session.execute(docs_stmt)

    run_to_docs: dict[UUID, list[UUID]] = {run_id: [] for run_id in run_ids}
    for run_id, document_id in docs_result:
        run_to_docs[run_id].append(document_id)

    schemas = [
        _to_run_schema(run, transcript_document_ids=run_to_docs[run.id])
        for run in runs
    ]

    return JSONResponse(
        content=ResponseEnvelope.ok(schemas).model_dump(mode="json")
    )


@router.get(
    "/codebook-application-runs/{run_id}",
    response_model=ResponseEnvelope[CodebookApplicationRunDetailSchema],
    summary="Get codebook application run",
)
async def get_codebook_application_run(
    run_id: UUID,
    session: DbSession,
) -> JSONResponse:
    run = await session.get(CodebookApplicationRun, run_id)
    if run is None:
        raise NotFoundError(f"Codebook application run '{run_id}' not found")
    document_codings = await _load_document_coding_schemas(run_id=run_id, session=session)
    transcript_ids = [dc.document_id for dc in document_codings]
    detail = CodebookApplicationRunDetailSchema(
        **_to_run_schema(run, transcript_document_ids=transcript_ids).model_dump(),
        document_codings=document_codings,
    )
    return JSONResponse(content=ResponseEnvelope.ok(detail).model_dump(mode="json"))


@router.get(
    "/codebook-application-runs/{run_id}/documents",
    response_model=ResponseEnvelope[list[DocumentCodingSchema]],
    summary="List document codings for a codebook application run",
)
async def list_codebook_application_run_documents(
    run_id: UUID,
    session: DbSession,
) -> JSONResponse:
    run = await session.get(CodebookApplicationRun, run_id)
    if run is None:
        raise NotFoundError(f"Codebook application run '{run_id}' not found")
    document_codings = await _load_document_coding_schemas(run_id=run_id, session=session)
    return JSONResponse(content=ResponseEnvelope.ok(document_codings).model_dump(mode="json"))


async def _load_document_coding_schemas(
    *,
    run_id: UUID,
    session: DbSession,
) -> list[DocumentCodingSchema]:
    document_codings = list(
        (
            await session.scalars(
                select(DocumentCoding)
                .where(DocumentCoding.application_run_id == run_id)
                .order_by(DocumentCoding.created_at)
            )
        ).all()
    )
    if not document_codings:
        return []

    document_coding_ids = [document_coding.id for document_coding in document_codings]
    theme_assignments = list(
        (
            await session.scalars(
                select(ThemeAssignment).where(
                    ThemeAssignment.document_coding_id.in_(document_coding_ids)
                )
            )
        ).all()
    )
    code_assignments = list(
        (
            await session.scalars(
                select(CodeAssignment).where(
                    CodeAssignment.document_coding_id.in_(document_coding_ids)
                )
            )
        ).all()
    )
    themes_by_document_coding_id: dict[UUID, list[ThemeAssignment]] = {}
    for theme_assignment in theme_assignments:
        themes_by_document_coding_id.setdefault(theme_assignment.document_coding_id, []).append(theme_assignment)
    codes_by_document_coding_id: dict[UUID, list[CodeAssignment]] = {}
    for code_assignment in code_assignments:
        codes_by_document_coding_id.setdefault(code_assignment.document_coding_id, []).append(code_assignment)

    return [
        _to_document_coding_schema(
            document_coding,
            theme_assignments=themes_by_document_coding_id.get(document_coding.id, []),
            code_assignments=codes_by_document_coding_id.get(document_coding.id, []),
        )
        for document_coding in document_codings
    ]

