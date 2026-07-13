from __future__ import annotations

import importlib.util
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import (
    Base,
    Codebook,
    CodebookApplicationJob,
    CodebookApplicationRun,
    CodebookGenerationJob,
    Corpus,
)
from app.services.codebook_application_jobs import CodebookApplicationJobRunner
from app.services.codebook_generation_jobs import CodebookGenerationJobRunner

AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None


@unittest.skipUnless(AIOSQLITE_AVAILABLE, "These tests require aiosqlite.")
class JobRunnerReconciliationTests(unittest.IsolatedAsyncioTestCase):
    """A backend restart clears in-memory job tasks but leaves DB rows behind.

    ``start()`` on each runner must sweep those orphaned rows so the UI never
    shows a progress bar for a job nothing is actually working on anymore.
    """

    async def asyncSetUp(self) -> None:
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def test_generation_runner_marks_orphaned_jobs_failed_on_start(self) -> None:
        corpus_id = uuid4()
        running_job_id = uuid4()
        queued_job_id = uuid4()
        finished_job_id = uuid4()
        async with self.session_factory() as session:
            session.add(Corpus(id=corpus_id, project_id=uuid4(), name="Corpus"))
            session.add(
                CodebookGenerationJob(
                    id=running_job_id,
                    status="running",
                    phase="synthesizing_themes",
                    codebook_name="Interview Codebook",
                    corpus_id=corpus_id,
                    transcript_document_ids_json="[]",
                )
            )
            session.add(
                CodebookGenerationJob(
                    id=queued_job_id,
                    status="queued",
                    phase="queued",
                    codebook_name="Another Codebook",
                    corpus_id=corpus_id,
                    transcript_document_ids_json="[]",
                )
            )
            session.add(
                CodebookGenerationJob(
                    id=finished_job_id,
                    status="succeeded",
                    phase="succeeded",
                    codebook_name="Finished Codebook",
                    corpus_id=corpus_id,
                    transcript_document_ids_json="[]",
                )
            )
            await session.commit()

        runner = CodebookGenerationJobRunner()
        with patch(
            "app.services.codebook_generation_jobs.get_session_factory",
            return_value=self.session_factory,
        ):
            await runner.reconcile_orphaned_jobs()

        async with self.session_factory() as session:
            running_job = await session.get(CodebookGenerationJob, running_job_id)
            queued_job = await session.get(CodebookGenerationJob, queued_job_id)
            finished_job = await session.get(CodebookGenerationJob, finished_job_id)

        self.assertEqual(running_job.status, "failed")
        self.assertEqual(running_job.phase, "failed")
        self.assertIsNotNone(running_job.error_message)
        self.assertIn("restart", running_job.error_message.lower())
        self.assertIsNotNone(running_job.finished_at)

        self.assertEqual(queued_job.status, "failed")
        self.assertIsNotNone(queued_job.finished_at)

        # Already-terminal jobs are left untouched.
        self.assertEqual(finished_job.status, "succeeded")
        self.assertIsNone(finished_job.error_message)

    async def test_generation_runner_reconcile_is_a_no_op_with_no_orphaned_jobs(self) -> None:
        runner = CodebookGenerationJobRunner()
        with patch(
            "app.services.codebook_generation_jobs.get_session_factory",
            return_value=self.session_factory,
        ):
            await runner.reconcile_orphaned_jobs()  # must not raise on an empty table

    async def test_application_runner_marks_orphaned_jobs_and_runs_failed_on_start(self) -> None:
        corpus_id = uuid4()
        codebook_id = uuid4()
        run_id = uuid4()
        running_job_id = uuid4()
        async with self.session_factory() as session:
            session.add(Corpus(id=corpus_id, project_id=uuid4(), name="Corpus"))
            session.add(
                Codebook(
                    id=codebook_id,
                    corpus_id=corpus_id,
                    name="Codebook",
                    description="Fixture",
                    version=1,
                    created_by="system",
                )
            )
            session.add(
                CodebookApplicationRun(
                    id=run_id,
                    corpus_id=corpus_id,
                    codebook_id=codebook_id,
                    status="running",
                    documents_total=3,
                )
            )
            session.add(
                CodebookApplicationJob(
                    id=running_job_id,
                    status="running",
                    phase="coding_documents",
                    corpus_id=corpus_id,
                    codebook_id=codebook_id,
                    transcript_document_ids_json="[]",
                    application_run_id=run_id,
                )
            )
            await session.commit()

        runner = CodebookApplicationJobRunner()
        with patch(
            "app.services.codebook_application_jobs.get_session_factory",
            return_value=self.session_factory,
        ):
            await runner.reconcile_orphaned_jobs()

        async with self.session_factory() as session:
            job = await session.get(CodebookApplicationJob, running_job_id)
            run = await session.get(CodebookApplicationRun, run_id)

        self.assertEqual(job.status, "failed")
        self.assertIsNotNone(job.error_message)
        self.assertIn("restart", job.error_message.lower())
        self.assertEqual(run.status, "failed")
        self.assertIsNotNone(run.finished_at)


if __name__ == "__main__":
    unittest.main()
