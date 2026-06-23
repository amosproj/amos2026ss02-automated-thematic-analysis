from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime
from uuid import UUID

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import get_session_factory
from app.models import CodebookGenerationJob
from app.services.app_settings import get_active_provider
from app.services.codebook_generation import (
    CodebookGenerationCancelledError,
    CodebookGenerationService,
)


def _utc_now_naive() -> datetime:
    # Models store naive UTC timestamps, matching the existing DB schema.
    return datetime.now(UTC).replace(tzinfo=None)


class CodebookGenerationJobRunner:
    _TERMINAL_PHASES = frozenset({"succeeded", "failed", "cancelled"})
    _TERMINAL_PHASE_TTL_S = 60.0 * 60.0
    _MAX_PHASE_ENTRIES = 4096

    def __init__(self) -> None:
        self._tasks: dict[UUID, asyncio.Task[None]] = {}
        self._phases: dict[UUID, str] = {}
        self._phase_updated_at: dict[UUID, float] = {}

    def get_phase(self, job_id: UUID, *, status: str) -> str:
        self._prune_phases()
        return self._phases.get(job_id, status)

    def set_phase(self, job_id: UUID, phase: str) -> None:
        self._prune_phases()
        self._phases[job_id] = phase
        self._phase_updated_at[job_id] = time.monotonic()
        self._prune_phases()

    def _prune_phases(self) -> None:
        now = time.monotonic()
        expired_job_ids = [
            phase_job_id
            for phase_job_id, phase in self._phases.items()
            if phase in self._TERMINAL_PHASES
            and (now - self._phase_updated_at.get(phase_job_id, now)) >= self._TERMINAL_PHASE_TTL_S
        ]
        for expired_job_id in expired_job_ids:
            self._phases.pop(expired_job_id, None)
            self._phase_updated_at.pop(expired_job_id, None)

        if len(self._phases) <= self._MAX_PHASE_ENTRIES:
            return

        terminal_job_ids = [
            phase_job_id for phase_job_id, phase in self._phases.items() if phase in self._TERMINAL_PHASES
        ]
        terminal_job_ids.sort(key=lambda phase_job_id: self._phase_updated_at.get(phase_job_id, 0.0))
        overflow = len(self._phases) - self._MAX_PHASE_ENTRIES
        for evict_job_id in terminal_job_ids[:overflow]:
            self._phases.pop(evict_job_id, None)
            self._phase_updated_at.pop(evict_job_id, None)

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        tasks = list(self._tasks.values())
        # Shutdown cancels in-flight background work instead of leaving orphaned
        # tasks attached to the application event loop.
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()
        self._phases.clear()
        self._phase_updated_at.clear()

    async def enqueue(
        self,
        job_id: UUID,
        *,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        if job_id in self._tasks and not self._tasks[job_id].done():
            return
        selected_factory = session_factory or get_session_factory()
        # The API returns immediately; generation continues in this tracked task.
        task = asyncio.create_task(
            self._run_one(job_id, selected_factory),
            name=f"codebook-generation-job-{job_id}",
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
            logger.exception("Unhandled error while processing codebook generation job {}", job_id)
        finally:
            self._tasks.pop(job_id, None)
            # Keep terminal phase available for polling finished jobs.

    async def _process_one(
        self,
        job_id: UUID,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:

        async with session_factory() as session:
            job = await session.get(CodebookGenerationJob, job_id)
            if job is None:
                return
            if job.status != "queued":
                return
            if job.cancel_requested:
                # A queued job can be cancelled before the worker starts it.
                job.status = "cancelled"
                self.set_phase(job_id, "cancelled")
                job.finished_at = _utc_now_naive()
                await session.commit()
                return

            job.status = "running"
            self.set_phase(job_id, "generating_passages")
            job.started_at = _utc_now_naive()
            await session.commit()

            transcript_document_ids = [UUID(raw) for raw in json.loads(job.transcript_document_ids_json)]
            # Bind the globally selected LLM provider at run start so the whole
            # job uses one consistent provider even if the setting changes mid-run.
            active_provider = await get_active_provider(session)
            service = CodebookGenerationService(session)

            async def _on_progress(done: int, total: int) -> None:
                # Use a short-lived session so progress writes are visible even
                # while the generation session is busy with LLM work.
                async with session_factory() as progress_session:
                    progress_job = await progress_session.get(CodebookGenerationJob, job_id)
                    if progress_job is None:
                        return
                    progress_job.passages_done = done
                    progress_job.passages_total = total
                    await progress_session.commit()

            async def _should_cancel() -> bool:
                # Poll cancellation from a fresh session to avoid stale ORM state.
                async with session_factory() as cancel_session:
                    cancel_job = await cancel_session.get(CodebookGenerationJob, job_id)
                    return bool(cancel_job and cancel_job.cancel_requested)

            async def _on_phase(phase: str) -> None:
                self.set_phase(job_id, phase)

            try:
                generated = await service.generate_codebook(
                    codebook_name=job.codebook_name,
                    corpus_id=job.corpus_id,
                    transcript_document_ids=transcript_document_ids,
                    research_query=job.research_query,
                    researcher_topics=job.researcher_topics,
                    provider=active_provider,
                    on_progress=_on_progress,
                    on_phase=_on_phase,
                    should_cancel=_should_cancel,
                )
                await session.refresh(job)
                job.status = "succeeded"
                self.set_phase(job_id, "succeeded")
                job.codebook_id = generated.codebook.id
                job.transcripts_processed = generated.transcripts_processed
                job.passages_processed = generated.passages_processed
                job.themes_created = generated.themes_created
                job.codes_created = generated.codes_created
                job.passages_done = generated.passages_processed
                job.passages_total = max(job.passages_total, generated.passages_processed)
                if generated.failed_passages:
                    # Successful jobs can still report passages skipped after
                    # repeated parser or validation failures.
                    job.error_message = json.dumps(
                        {
                            "type": "passage_generation_partial_failures",
                            "passages_failed": generated.passages_failed,
                            "failed_passages": [failure.model_dump(mode="json") for failure in generated.failed_passages],
                        },
                        ensure_ascii=False,
                    )
                else:
                    job.error_message = None
                job.finished_at = _utc_now_naive()
                await session.commit()
            except CodebookGenerationCancelledError:
                await session.rollback()
                await session.refresh(job)
                job.status = "cancelled"
                self.set_phase(job_id, "cancelled")
                job.finished_at = _utc_now_naive()
                await session.commit()
            except Exception as exc:
                await session.rollback()
                await session.refresh(job)
                job.status = "failed"
                self.set_phase(job_id, "failed")
                job.error_message = str(exc)
                job.finished_at = _utc_now_naive()
                await session.commit()


codebook_generation_job_runner = CodebookGenerationJobRunner()
