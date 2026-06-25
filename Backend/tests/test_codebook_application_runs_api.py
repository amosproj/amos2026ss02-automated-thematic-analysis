"""API tests for deleting codebook application runs (issue #203)."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Codebook, CodebookApplicationRun, Corpus

RUNS_API = "/api/v1/codebook-application-runs"


def _now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


async def _seed_run(db_engine, *, status: str = "succeeded") -> str:
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        corpus = Corpus(id=uuid4(), project_id=uuid4(), name="Run Corpus")
        codebook = Codebook(
            id=uuid4(),
            corpus_id=corpus.id,
            name="Run Codebook",
            description="Fixture",
            version=1,
            created_by="system",
        )
        run = CodebookApplicationRun(
            id=uuid4(),
            name="Initial Run",
            custom_id="RUN-001",
            corpus_id=corpus.id,
            codebook_id=codebook.id,
            status=status,
            documents_total=2,
            documents_coded=2 if status == "succeeded" else 0,
            documents_failed=0,
            started_at=_now_naive(),
            finished_at=_now_naive() if status != "running" else None,
        )
        session.add_all([corpus, codebook, run])
        await session.commit()
        return str(run.id)


async def test_delete_run_succeeds_and_removes_it(client, db_engine) -> None:
    run_id = await _seed_run(db_engine)

    response = await client.delete(f"{RUNS_API}/{run_id}")
    assert response.status_code == 200
    assert response.json()["success"] is True

    # The run is gone; a follow-up fetch 404s.
    follow_up = await client.get(f"{RUNS_API}/{run_id}")
    assert follow_up.status_code == 404


async def test_delete_unknown_run_is_404(client) -> None:
    response = await client.delete(f"{RUNS_API}/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["success"] is False


async def test_delete_running_run_is_rejected(client, db_engine) -> None:
    run_id = await _seed_run(db_engine, status="running")

    response = await client.delete(f"{RUNS_API}/{run_id}")
    assert response.status_code == 422
    assert response.json()["success"] is False

    # The run is untouched and still retrievable.
    follow_up = await client.get(f"{RUNS_API}/{run_id}")
    assert follow_up.status_code == 200
