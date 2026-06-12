from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import get_session_factory
from app.models import CodebookApplicationJob, CodebookApplicationRun
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
            service = CodebookApplicationService(session)

            async def _on_progress(done: int, total: int) -> None:
                async with session_factory() as progress_session:
                    progress_job = await progress_session.get(CodebookApplicationJob, job_id)
                    if progress_job is None:
                        return
                    progress_job.documents_done = done
                    progress_job.documents_total = total
                    await progress_session.commit()

            async def _on_phase(phase: str) -> None:
                async with session_factory() as phase_session:
                    phase_job = await phase_session.get(CodebookApplicationJob, job_id)
                    if phase_job is None:
                        return
                    phase_job.phase = phase
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
                    return bool(cancel_job and cancel_job.cancel_requested)

            try:
                summary = await service.apply_codebook(
                    corpus_id=job.corpus_id,
                    codebook_id=job.codebook_id,
                    transcript_document_ids=transcript_document_ids,
                    on_progress=_on_progress,
                    on_phase=_on_phase,
                    on_run_created=_on_run_created,
                    should_cancel=_should_cancel,
                )
                await session.refresh(job)
                job.status = "succeeded"
                job.phase = "succeeded"
                job.application_run_id = summary.application_run.id
                job.documents_total = summary.documents_total
                job.documents_done = summary.documents_total
                job.documents_coded = summary.documents_coded
                job.documents_failed = summary.documents_failed
                job.error_message = (
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
                job.finished_at = _utc_now_naive()
                await session.commit()
            except CodebookApplicationCancelledError:
                await session.rollback()
                await session.refresh(job)
                job.status = "cancelled"
                job.phase = "cancelled"
                job.finished_at = _utc_now_naive()
                await self._mark_run_terminal(session, job.application_run_id, status="cancelled")
                await session.commit()
            except Exception as exc:
                await session.rollback()
                await session.refresh(job)
                job.status = "failed"
                job.phase = "failed"
                job.error_message = str(exc)
                job.finished_at = _utc_now_naive()
                await self._mark_run_terminal(session, job.application_run_id, status="failed")
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

