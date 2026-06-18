from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import get_session_factory
from app.models import TraceableAnalysisJob
from app.services.traceable_analysis import (
    TraceableAnalysisCancelledError,
    TraceableAnalysisService,
)


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class TraceableAnalysisJobRunner:
    def __init__(self) -> None:
        self._tasks: dict[UUID, asyncio.Task[None]] = {}

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()

    async def enqueue(
        self,
        job_id: UUID,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        if job_id in self._tasks and not self._tasks[job_id].done():
            return
        selected_factory = session_factory or get_session_factory()
        task = asyncio.create_task(
            self._run_one(job_id, selected_factory),
            name=f"traceable-analysis-job-{job_id}",
        )
        self._tasks[job_id] = task

    async def _run_one(
        self,
        job_id: UUID,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        try:
            await self._process_one(job_id, session_factory)
        except Exception:
            logger.exception("Unhandled error while processing traceable analysis job {}", job_id)
        finally:
            self._tasks.pop(job_id, None)

    async def _process_one(
        self,
        job_id: UUID,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        async with session_factory() as session:
            job = await session.get(TraceableAnalysisJob, job_id)
            if job is None or job.status != "queued":
                return
            if job.cancel_requested:
                job.status = "cancelled"
                job.phase = "cancelled"
                job.finished_at = _utc_now_naive()
                await session.commit()
                return

            job.status = "running"
            job.phase = "extracting_quote_codes"
            job.started_at = _utc_now_naive()
            await session.commit()
            logger.info(
                "Traceable analysis job started: job_id={}, corpus_id={}, codebook_name='{}'",
                job.id,
                job.corpus_id,
                job.codebook_name,
            )

            transcript_document_ids = [UUID(raw) for raw in json.loads(job.transcript_document_ids_json)]
            service = TraceableAnalysisService(session)

            async def _on_unit_progress(done: int, total: int) -> None:
                async with session_factory() as progress_session:
                    progress_job = await progress_session.get(TraceableAnalysisJob, job_id)
                    if progress_job is None:
                        return
                    progress_job.analysis_units_done = done
                    progress_job.analysis_units_total = total
                    progress_job.documents_done = done
                    progress_job.documents_total = total
                    await progress_session.commit()

            async def _on_phase_progress(phase: str, done: int, total: int) -> None:
                async with session_factory() as progress_session:
                    progress_job = await progress_session.get(TraceableAnalysisJob, job_id)
                    if progress_job is None:
                        return
                    progress_job.phase = phase
                    progress_job.analysis_units_done = done
                    progress_job.analysis_units_total = total
                    await progress_session.commit()

            async def _on_phase(phase: str) -> None:
                async with session_factory() as phase_session:
                    phase_job = await phase_session.get(TraceableAnalysisJob, job_id)
                    if phase_job is None:
                        return
                    phase_job.phase = phase
                    phase_job.analysis_units_done = 0
                    phase_job.analysis_units_total = 0
                    await phase_session.commit()
                logger.info("Traceable analysis job phase changed: job_id={}, phase={}", job_id, phase)

            async def _on_codebook_created(codebook_id: UUID) -> None:
                async with session_factory() as codebook_session:
                    codebook_job = await codebook_session.get(TraceableAnalysisJob, job_id)
                    if codebook_job is None:
                        return
                    codebook_job.codebook_id = codebook_id
                    await codebook_session.commit()

            async def _on_application_run_created(application_run_id: UUID) -> None:
                async with session_factory() as run_session:
                    run_job = await run_session.get(TraceableAnalysisJob, job_id)
                    if run_job is None:
                        return
                    run_job.application_run_id = application_run_id
                    await run_session.commit()

            async def _should_cancel() -> bool:
                async with session_factory() as cancel_session:
                    cancel_job = await cancel_session.get(TraceableAnalysisJob, job_id)
                    return bool(cancel_job and cancel_job.cancel_requested)

            try:
                result = await service.run_analysis(
                    codebook_name=job.codebook_name,
                    analysis_name=job.analysis_name,
                    custom_id=job.custom_id,
                    corpus_id=job.corpus_id,
                    transcript_document_ids=transcript_document_ids,
                    research_query=job.research_query,
                    researcher_topics=job.researcher_topics,
                    max_refinement_rounds=job.max_refinement_rounds,
                    on_unit_progress=_on_unit_progress,
                    on_phase_progress=_on_phase_progress,
                    on_phase=_on_phase,
                    on_codebook_created=_on_codebook_created,
                    on_application_run_created=_on_application_run_created,
                    should_cancel=_should_cancel,
                )
                await session.refresh(job)
                job.status = "succeeded"
                job.phase = "succeeded"
                job.codebook_id = result.codebook_id
                job.application_run_id = result.application_run_id
                job.documents_total = result.documents_processed
                job.documents_done = result.documents_processed
                job.analysis_units_total = result.analysis_units_processed
                job.analysis_units_done = result.analysis_units_processed
                job.quotes_created = result.quotes_created
                job.codes_created = result.codes_created
                job.themes_created = result.themes_created
                job.documents_coded = result.documents_coded
                job.documents_failed = result.documents_failed
                job.provenance_json = json.dumps(result.provenance, ensure_ascii=False)
                job.action_log_json = json.dumps(result.action_log, ensure_ascii=False)
                job.error_message = None
                job.finished_at = _utc_now_naive()
                await session.commit()
                logger.info(
                    "Traceable analysis job succeeded: job_id={}, codebook_id={}, application_run_id={}, "
                    "quotes={}, codes={}, themes={}, documents_coded={}",
                    job_id,
                    result.codebook_id,
                    result.application_run_id,
                    result.quotes_created,
                    result.codes_created,
                    result.themes_created,
                    result.documents_coded,
                )
            except TraceableAnalysisCancelledError:
                await session.rollback()
                await session.refresh(job)
                job.status = "cancelled"
                job.phase = "cancelled"
                job.finished_at = _utc_now_naive()
                await session.commit()
                logger.info("Traceable analysis job cancelled: job_id={}", job_id)
            except Exception as exc:
                await session.rollback()
                await session.refresh(job)
                job.status = "failed"
                job.phase = "failed"
                job.error_message = str(exc)
                job.finished_at = _utc_now_naive()
                await session.commit()
                logger.exception("Traceable analysis job failed: job_id={}, error={}", job_id, exc)


traceable_analysis_job_runner = TraceableAnalysisJobRunner()
