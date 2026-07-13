from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID

from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import get_session_factory
from app.models import CodebookApplicationJob, CodebookApplicationRun
from app.services.app_settings import get_active_provider
from app.services.codebook_application import (
    CodebookApplicationCancelledError,
    CodebookApplicationService,
)


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class CodebookApplicationJobRunner:
    def __init__(self) -> None:
        self._tasks: dict[UUID, asyncio.Task[None]] = {}

    async def start(self) -> None:
        # Kept as a no-op: routers also call start() defensively before every
        # enqueue() (in case app lifespan startup was skipped), so it must
        # stay safe to call repeatedly mid-request. Orphaned-job reconciliation
        # lives in reconcile_orphaned_jobs() instead, called exactly once from
        # app lifespan — never from a request handler.
        return

    async def reconcile_orphaned_jobs(self) -> None:
        # Mirrors CodebookGenerationJobRunner.reconcile_orphaned_jobs(): jobs
        # left "queued"/"running" belong to a worker task that no longer
        # exists in this process after a restart, so their DB status would
        # otherwise never leave queued/running and the UI would show a
        # progress bar that can never finish.
        session_factory = get_session_factory()
        async with session_factory() as session:
            orphaned_jobs = list(
                (
                    await session.scalars(
                        select(CodebookApplicationJob).where(
                            CodebookApplicationJob.status.in_(("queued", "running"))
                        )
                    )
                ).all()
            )
            if not orphaned_jobs:
                return
            now = _utc_now_naive()
            for job in orphaned_jobs:
                job.status = "failed"
                job.phase = "failed"
                job.error_message = "Codebook application was interrupted by a server restart."
                job.finished_at = now
                if job.application_run_id is not None:
                    run = await session.get(CodebookApplicationRun, job.application_run_id)
                    if run is not None:
                        run.status = "failed"
                        run.finished_at = now
            await session.commit()
            logger.warning(
                "Reconciled {} orphaned codebook application job(s) on startup",
                len(orphaned_jobs),
            )

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
            name=f"codebook-application-job-{job_id}",
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
            logger.exception("Unhandled error while processing codebook application job {}", job_id)
        finally:
            self._tasks.pop(job_id, None)

    async def _process_one(
        self,
        job_id: UUID,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        async with session_factory() as session:
            job = await session.get(CodebookApplicationJob, job_id)
            if job is None:
                return
            if job.status != "queued":
                return
            if job.cancel_requested:
                job.status = "cancelled"
                job.phase = "cancelled"
                job.finished_at = _utc_now_naive()
                await session.commit()
                return

            job.status = "running"
            job.phase = "loading_codebook"
            job.started_at = _utc_now_naive()
            await session.commit()

            transcript_document_ids = [UUID(raw) for raw in json.loads(job.transcript_document_ids_json)]
            # Bind the globally selected LLM provider at run start so the whole
            # job uses one consistent provider even if the setting changes
            # mid-run. Read it via a short-lived session so this lookup doesn't
            # leave a transaction open on the long-lived job session.
            async with session_factory() as provider_session:
                active_provider = await get_active_provider(provider_session)
            service = CodebookApplicationService(session)

            async def _on_progress(done: int, total: int,
                                   coded: int | None = None, failed: int | None = None) -> None:
                async with session_factory() as progress_session:
                    progress_job = await progress_session.get(CodebookApplicationJob, job_id)
                    if progress_job is None:
                        return
                    progress_job.documents_done = done
                    progress_job.documents_total = total
                    if coded is not None:  # stream coded/failed live, not just at the end
                        progress_job.documents_coded = coded
                    if failed is not None:
                        progress_job.documents_failed = failed
                    progress_job.llm_tokens_input = service.traceable_service.llm_tokens_input
                    progress_job.llm_tokens_output = service.traceable_service.llm_tokens_output
                    await progress_session.commit()

            async def _on_phase(phase: str) -> None:
                async with session_factory() as phase_session:
                    phase_job = await phase_session.get(CodebookApplicationJob, job_id)
                    if phase_job is None:
                        return
                    phase_job.phase = phase
                    phase_job.llm_tokens_input = service.traceable_service.llm_tokens_input
                    phase_job.llm_tokens_output = service.traceable_service.llm_tokens_output
                    await phase_session.commit()

            async def _on_run_created(application_run_id: UUID) -> None:
                async with session_factory() as run_session:
                    run_job = await run_session.get(CodebookApplicationJob, job_id)
                    if run_job is None:
                        return
                    run_job.application_run_id = application_run_id
                    await run_session.commit()

            async def _should_cancel() -> bool:
                async with session_factory() as cancel_session:
                    cancel_job = await cancel_session.get(CodebookApplicationJob, job_id)
                    return cancel_job is None or cancel_job.cancel_requested

            try:
                summary = await service.apply_codebook(
                    name=job.name,
                    custom_id=job.custom_id,
                    corpus_id=job.corpus_id,
                    codebook_id=job.codebook_id,
                    transcript_document_ids=transcript_document_ids,
                    provider=active_provider,
                    on_progress=_on_progress,
                    on_phase=_on_phase,
                    on_run_created=_on_run_created,
                    should_cancel=_should_cancel,
                )
                current_job = await session.get(CodebookApplicationJob, job_id)
                if current_job is None:
                    return
                if current_job.cancel_requested:
                    current_job.status = "cancelled"
                    current_job.phase = "cancelled"
                    current_job.finished_at = _utc_now_naive()
                    await self._mark_run_terminal(session, current_job.application_run_id, status="cancelled")
                    await session.commit()
                    return
                current_job.status = "succeeded"
                current_job.phase = "succeeded"
                current_job.application_run_id = summary.application_run.id
                current_job.documents_total = summary.documents_total
                current_job.documents_done = summary.documents_total
                current_job.documents_coded = summary.documents_coded
                current_job.documents_failed = summary.documents_failed
                current_job.llm_tokens_input = service.traceable_service.llm_tokens_input
                current_job.llm_tokens_output = service.traceable_service.llm_tokens_output
                current_job.error_message = (
                    json.dumps(
                        {
                            "type": "document_application_partial_failures",
                            "documents_failed": summary.documents_failed,
                            "failed_documents": summary.failed_documents,
                        },
                        ensure_ascii=False,
                    )
                    if summary.documents_failed
                    else None
                )
                current_job.finished_at = _utc_now_naive()
                await session.commit()
            except CodebookApplicationCancelledError:
                await session.rollback()
                current_job = await session.get(CodebookApplicationJob, job_id)
                if current_job is None:
                    return
                current_job.status = "cancelled"
                current_job.phase = "cancelled"
                current_job.finished_at = _utc_now_naive()
                await self._mark_run_terminal(session, current_job.application_run_id, status="cancelled")
                await session.commit()
            except Exception as exc:
                await session.rollback()
                current_job = await session.get(CodebookApplicationJob, job_id)
                if current_job is None:
                    return
                if current_job.cancel_requested:
                    current_job.status = "cancelled"
                    current_job.phase = "cancelled"
                    current_job.finished_at = _utc_now_naive()
                    await self._mark_run_terminal(session, current_job.application_run_id, status="cancelled")
                    await session.commit()
                    return
                current_job.status = "failed"
                current_job.phase = "failed"
                current_job.error_message = str(exc)
                current_job.finished_at = _utc_now_naive()
                await self._mark_run_terminal(session, current_job.application_run_id, status="failed")
                await session.commit()

    @staticmethod
    async def _mark_run_terminal(
        session: AsyncSession,
        application_run_id: UUID | None,
        *,
        status: str,
    ) -> None:
        if application_run_id is None:
            return
        run = await session.get(CodebookApplicationRun, application_run_id)
        if run is None:
            return
        run.status = status
        run.finished_at = _utc_now_naive()


codebook_application_job_runner = CodebookApplicationJobRunner()

