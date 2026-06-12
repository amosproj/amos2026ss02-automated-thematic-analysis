from __future__ import annotations

import asyncio
import json
import time
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import (
    Code,
    Codebook,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Corpus,
    CorpusDocument,
    Theme,
    ThemeCodeRelationship,
)
from app.schemas.llm import AppliedCodeAssignment, AppliedThemeAssignment, CodebookApplicationResult
from app.services.codebook_application import CodebookApplicationService

API_CODEBOOKS = "/api/v1/codebooks"


async def _seed_corpus_codebook(db_engine, texts: list[str]) -> tuple[str, str, list[str]]:
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        corpus = Corpus(id=uuid4(), project_id=uuid4(), name="Application Corpus")
        session.add(corpus)
        await session.flush()

        documents = [
            CorpusDocument(
                id=uuid4(),
                corpus_id=corpus.id,
                title=f"Doc {index}",
                content=text,
            )
            for index, text in enumerate(texts, start=1)
        ]
        session.add_all(documents)

        codebook = Codebook(
            id=uuid4(),
            corpus_id=corpus.id,
            name="Application Codebook",
            description="Fixture",
            version=1,
            created_by="system",
        )
        theme = Theme(
            id=uuid4(),
            codebook_id=codebook.id,
            label="Workflow Friction",
            description="Friction caused by workflow design.",
            is_active=True,
        )
        code = Code(
            id=uuid4(),
            codebook_id=codebook.id,
            label="Manual Handoffs",
            description="Manual handoffs slow work.",
            is_active=True,
        )
        session.add_all([codebook, theme, code])
        await session.flush()

        session.add_all([
            CodebookThemeRelationship(
                id=uuid4(),
                codebook_id=codebook.id,
                theme_id=theme.id,
                is_active=True,
            ),
            CodebookCodeRelationship(
                id=uuid4(),
                codebook_id=codebook.id,
                code_id=code.id,
                is_active=True,
            ),
            ThemeCodeRelationship(
                id=uuid4(),
                codebook_id=codebook.id,
                theme_id=theme.id,
                code_id=code.id,
                is_active=True,
            ),
        ])
        await session.commit()
        return str(corpus.id), str(codebook.id), [str(document.id) for document in documents]


async def _wait_for_terminal_job_status(client, job_id: str, timeout_seconds: float = 10.0) -> dict:
    started = time.monotonic()
    last_payload: dict = {}
    # Give the async worker a chance to complete its final write before the test
    # starts opening repeated read transactions against shared-cache SQLite.
    await asyncio.sleep(0.1)
    while time.monotonic() - started < timeout_seconds:
        response = await client.get(f"{API_CODEBOOKS}/apply-jobs/{job_id}")
        assert response.status_code == 200
        payload = response.json()["data"]
        last_payload = payload
        if payload["status"] in {"succeeded", "failed", "cancelled"}:
            return payload
        await asyncio.sleep(0.1)
    raise AssertionError(f"Job {job_id} did not reach terminal status. Last payload: {last_payload}")


def _application_result(quote: str = "manual handoffs slow") -> CodebookApplicationResult:
    return CodebookApplicationResult(
        summary="The participant describes workflow friction.",
        researcher_notes=None,
        themes=[
            AppliedThemeAssignment(
                theme_label="Workflow Friction",
                present=True,
                confidence=0.9,
                quote=quote,
            )
        ],
        codes=[
            AppliedCodeAssignment(
                code_label="Manual Handoffs",
                theme_label="Workflow Friction",
                quote=quote,
                confidence=0.95,
                rationale="The quote directly describes slow manual handoffs.",
            )
        ],
    )


def _patch_application(monkeypatch, single_transcript_fn) -> None:
    async def _fake_apply(transcript: str, codebook_context: str, *_, **__):
        del codebook_context
        return single_transcript_fn(transcript)

    monkeypatch.setattr(
        "app.services.codebook_application.apply_codebook_with_codes_to_transcript",
        _fake_apply,
    )
    monkeypatch.setattr(
        "app.services.codebook_application.build_codebook_application_with_codes_chain",
        lambda: object(),
    )
    monkeypatch.setattr(
        CodebookApplicationService,
        "_compute_retry_delay",
        staticmethod(lambda *, attempt: 0.0),
    )


async def test_apply_codebook_job_persists_coding_and_quote_spans(client, db_engine, monkeypatch) -> None:
    corpus_id, codebook_id, document_ids = await _seed_corpus_codebook(
        db_engine,
        texts=["Participant: The manual handoffs slow everyone down."],
    )
    _patch_application(monkeypatch, lambda _: _application_result())

    create_response = await client.post(
        f"{API_CODEBOOKS}/{codebook_id}/apply-jobs",
        json={"transcript_document_ids": document_ids},
    )

    assert create_response.status_code == 202
    created_job = create_response.json()["data"]
    assert created_job["corpus_id"] == corpus_id
    terminal_job = await _wait_for_terminal_job_status(client, created_job["id"])
    assert terminal_job["status"] == "succeeded", terminal_job.get("error_message")
    assert terminal_job["application_run_id"] is not None
    assert terminal_job["documents_coded"] == 1
    assert terminal_job["documents_failed"] == 0

    documents_response = await client.get(
        f"/api/v1/codebook-application-runs/{terminal_job['application_run_id']}/documents"
    )
    assert documents_response.status_code == 200
    document_coding = documents_response.json()["data"][0]
    assert document_coding["status"] == "coded"
    assert len(document_coding["code_assignments"]) == 1
    code_assignment = document_coding["code_assignments"][0]
    assert code_assignment["quote"] == "manual handoffs slow"
    assert code_assignment["quote_match_status"] == "exact"
    assert code_assignment["start_char"] is not None
    assert code_assignment["end_char"] is not None


async def test_apply_codebook_job_retries_llm_and_fails_only_one_transcript(client, db_engine, monkeypatch) -> None:
    corpus_id, codebook_id, document_ids = await _seed_corpus_codebook(
        db_engine,
        texts=[
            "Participant: The manual handoffs slow everyone down.",
            "Participant: This transcript should trigger provider failure.",
        ],
    )
    calls_by_transcript: dict[str, int] = {}

    def _fake_result(transcript: str) -> CodebookApplicationResult:
        calls_by_transcript[transcript] = calls_by_transcript.get(transcript, 0) + 1
        if "provider failure" in transcript:
            raise RuntimeError("provider timeout")
        return _application_result()

    _patch_application(monkeypatch, _fake_result)

    create_response = await client.post(
        f"{API_CODEBOOKS}/{codebook_id}/apply-jobs",
        json={"transcript_document_ids": document_ids},
    )

    assert create_response.status_code == 202
    terminal_job = await _wait_for_terminal_job_status(client, create_response.json()["data"]["id"])
    assert terminal_job["status"] == "succeeded"
    assert terminal_job["documents_coded"] == 1
    assert terminal_job["documents_failed"] == 1
    warning = json.loads(terminal_job["error_message"])
    assert warning["type"] == "document_application_partial_failures"
    failed_transcript = "Participant: This transcript should trigger provider failure."
    assert calls_by_transcript[failed_transcript] == 3

    documents_response = await client.get(
        f"/api/v1/codebook-application-runs/{terminal_job['application_run_id']}/documents"
    )
    statuses = {row["status"] for row in documents_response.json()["data"]}
    assert statuses == {"coded", "failed"}


async def test_apply_codebook_job_creates_new_run_without_overwrite(client, db_engine, monkeypatch) -> None:
    corpus_id, codebook_id, document_ids = await _seed_corpus_codebook(
        db_engine,
        texts=["Participant: The manual handoffs slow everyone down."],
    )
    _patch_application(monkeypatch, lambda _: _application_result())

    run_ids: list[str] = []
    for _ in range(2):
        create_response = await client.post(
            f"{API_CODEBOOKS}/{codebook_id}/apply-jobs",
            json={"transcript_document_ids": document_ids},
        )
        assert create_response.status_code == 202
        terminal_job = await _wait_for_terminal_job_status(client, create_response.json()["data"]["id"])
        assert terminal_job["status"] == "succeeded"
        run_ids.append(terminal_job["application_run_id"])

    assert len(set(run_ids)) == 2
    runs_response = await client.get(f"{API_CODEBOOKS}/{codebook_id}/application-runs")
    assert runs_response.status_code == 200
    returned_run_ids = {row["id"] for row in runs_response.json()["data"]}
    assert set(run_ids).issubset(returned_run_ids)


async def test_apply_codebook_job_can_be_cancelled_while_running(client, db_engine, monkeypatch) -> None:
    corpus_id, codebook_id, document_ids = await _seed_corpus_codebook(
        db_engine,
        texts=[
            "Participant: The manual handoffs slow everyone down.",
            "Participant: More manual handoffs slow the team down.",
        ],
    )
    del corpus_id

    async def _slow_apply(transcript: str, codebook_context: str, *_, **__):
        del transcript, codebook_context
        await asyncio.sleep(0.05)
        return _application_result()

    monkeypatch.setattr(
        "app.services.codebook_application.apply_codebook_with_codes_to_transcript",
        _slow_apply,
    )
    monkeypatch.setattr(
        "app.services.codebook_application.build_codebook_application_with_codes_chain",
        lambda: object(),
    )
    monkeypatch.setattr(
        CodebookApplicationService,
        "_compute_retry_delay",
        staticmethod(lambda *, attempt: 0.0),
    )

    create_response = await client.post(
        f"{API_CODEBOOKS}/{codebook_id}/apply-jobs",
        json={"transcript_document_ids": document_ids},
    )
    assert create_response.status_code == 202
    created_job = create_response.json()["data"]

    cancel_response = await client.post(
        f"{API_CODEBOOKS}/apply-jobs/{created_job['id']}/cancel",
    )
    assert cancel_response.status_code == 202
    assert cancel_response.json()["data"]["cancel_requested"] is True
    terminal_job = await _wait_for_terminal_job_status(client, created_job["id"])
    assert terminal_job["status"] == "cancelled"


async def test_apply_codebook_job_rejects_document_ids_outside_corpus(client, db_engine) -> None:
    _, codebook_id, _ = await _seed_corpus_codebook(
        db_engine,
        texts=["Participant: The manual handoffs slow everyone down."],
    )

    response = await client.post(
        f"{API_CODEBOOKS}/{codebook_id}/apply-jobs",
        json={"transcript_document_ids": [str(uuid4())]},
    )

    assert response.status_code == 422


async def test_apply_codebook_job_rejects_conflicting_deprecated_corpus_id(client, db_engine) -> None:
    _, codebook_id, _ = await _seed_corpus_codebook(
        db_engine,
        texts=["Participant: The manual handoffs slow everyone down."],
    )

    response = await client.post(
        f"{API_CODEBOOKS}/{codebook_id}/apply-jobs",
        json={"corpus_id": str(uuid4())},
    )

    assert response.status_code == 422

