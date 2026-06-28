from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import ConflictError
from app.models import CodebookApplicationJob

_ACTIVE_STATUSES = {"queued", "running"}


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _document_ids_for_job(job: CodebookApplicationJob) -> set[UUID]:
    try:
        raw_ids = json.loads(job.transcript_document_ids_json or "[]")
    except json.JSONDecodeError:
        return set()
    return {UUID(raw_id) for raw_id in raw_ids}


def _job_label(job: CodebookApplicationJob) -> str:
    return job.name or job.custom_id or str(job.id)


def _conflict_message(*, resource_label: str, jobs: list[CodebookApplicationJob]) -> str:
    names = ", ".join(_job_label(job) for job in jobs[:3])
    suffix = f": {names}." if names else "."
    more = f" and {len(jobs) - 3} more" if len(jobs) > 3 else ""
    if more and suffix.endswith("."):
        suffix = suffix[:-1] + more + "."
    return f"Deleting {resource_label} will cancel the running analysis run(s){suffix}"


async def _cancel_jobs(jobs: Iterable[CodebookApplicationJob]) -> None:
    now = _utc_now_naive()
    for job in jobs:
        job.cancel_requested = True
        if job.status == "queued":
            job.status = "cancelled"
            job.phase = "cancelled"
            job.finished_at = now


async def active_jobs_for_documents(
    session: AsyncSession,
    *,
    corpus_id: UUID,
    document_ids: Iterable[UUID],
) -> list[CodebookApplicationJob]:
    requested_ids = set(document_ids)
    if not requested_ids:
        return []

    jobs = list(
        (
            await session.scalars(
                select(CodebookApplicationJob).where(
                    CodebookApplicationJob.corpus_id == corpus_id,
                    CodebookApplicationJob.status.in_(_ACTIVE_STATUSES),
                )
            )
        ).all()
    )

    impacted: list[CodebookApplicationJob] = []
    for job in jobs:
        job_document_ids = _document_ids_for_job(job)
        if not job_document_ids or requested_ids.intersection(job_document_ids):
            impacted.append(job)
    return impacted


async def guard_document_deletion(
    session: AsyncSession,
    *,
    corpus_id: UUID,
    document_ids: Iterable[UUID],
    force: bool,
) -> None:
    impacted_jobs = await active_jobs_for_documents(
        session,
        corpus_id=corpus_id,
        document_ids=document_ids,
    )
    if not impacted_jobs:
        return
    if not force:
        raise ConflictError(_conflict_message(resource_label="the selected transcript(s)", jobs=impacted_jobs))
    await _cancel_jobs(impacted_jobs)


async def active_jobs_for_codebooks(
    session: AsyncSession,
    *,
    codebook_ids: Iterable[UUID],
) -> list[CodebookApplicationJob]:
    requested_ids = set(codebook_ids)
    if not requested_ids:
        return []
    return list(
        (
            await session.scalars(
                select(CodebookApplicationJob).where(
                    CodebookApplicationJob.codebook_id.in_(requested_ids),
                    CodebookApplicationJob.status.in_(_ACTIVE_STATUSES),
                )
            )
        ).all()
    )


async def guard_codebook_deletion(
    session: AsyncSession,
    *,
    codebook_ids: Iterable[UUID],
    force: bool,
) -> None:
    impacted_jobs = await active_jobs_for_codebooks(session, codebook_ids=codebook_ids)
    if not impacted_jobs:
        return
    if not force:
        raise ConflictError(_conflict_message(resource_label="the selected codebook(s)", jobs=impacted_jobs))
    await _cancel_jobs(impacted_jobs)


async def active_jobs_for_corpus(
    session: AsyncSession,
    *,
    corpus_id: UUID,
) -> list[CodebookApplicationJob]:
    return list(
        (
            await session.scalars(
                select(CodebookApplicationJob).where(
                    CodebookApplicationJob.corpus_id == corpus_id,
                    CodebookApplicationJob.status.in_(_ACTIVE_STATUSES),
                )
            )
        ).all()
    )


async def guard_corpus_deletion(
    session: AsyncSession,
    *,
    corpus_id: UUID,
    force: bool,
) -> None:
    # Deleting a corpus cascades to every document it owns, so any active job
    # in the corpus is impacted regardless of which documents it targets.
    impacted_jobs = await active_jobs_for_corpus(session, corpus_id=corpus_id)
    if not impacted_jobs:
        return
    if not force:
        raise ConflictError(_conflict_message(resource_label="this corpus", jobs=impacted_jobs))
    await _cancel_jobs(impacted_jobs)
