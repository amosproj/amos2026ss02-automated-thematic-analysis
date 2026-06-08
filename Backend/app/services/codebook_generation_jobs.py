from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import get_session_factory
from app.models import CodebookGenerationJob
from app.services.codebook_generation import (
    CodebookGenerationCancelledError,
    CodebookGenerationService,
)


def _utc_now_naive() -> datetime:
    # Models store naive UTC timestamps, matching the existing DB schema.
    return datetime.now(UTC).replace(tzinfo=None)


class CodebookGenerationJobRunner:
    def __init__(self) -> None:
        self._tasks: dict[UUID, asyncio.Task[None]] = {}

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
                job.finished_at = _utc_now_naive()
                await session.commit()
                return

            job.status = "running"
            job.started_at = _utc_now_naive()
            await session.commit()

            transcript_document_ids = [UUID(raw) for raw in json.loads(job.transcript_document_ids_json)]
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

            try:
                generated = await service.generate_codebook(
                    codebook_name=job.codebook_name,
                    corpus_id=job.corpus_id,
                    transcript_document_ids=transcript_document_ids,
                    research_query=job.research_query,
                    researcher_topics=job.researcher_topics,
                    on_progress=_on_progress,
                    should_cancel=_should_cancel,
                )
                await session.refresh(job)
                job.status = "succeeded"
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
                job.finished_at = _utc_now_naive()
                await session.commit()
            except Exception as exc:
                await session.rollback()
                await session.refresh(job)
                job.status = "failed"
                job.error_message = str(exc)
                job.finished_at = _utc_now_naive()
                await session.commit()


codebook_generation_job_runner = CodebookGenerationJobRunner()
