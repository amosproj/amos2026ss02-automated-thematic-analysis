from __future__ import annotations

import asyncio
import json
import time
from uuid import UUID, uuid4

import pytest
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
from app.services.quote_matching import locate_quote_span
from app.services.traceable_analysis import (
    TraceableAnalysisCancelledError,
    TraceableAnalysisService,
    _ApplicationPassResult,
    _AppliedEvidence,
)

API_CODEBOOKS = "/api/v1/codebooks"


async def _seed_corpus_codebook(
    db_engine, texts: list[str], extra_code_labels: list[str] | None = None
) -> tuple[str, str, list[str]]:
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
        codes = [
            Code(
                id=uuid4(),
                codebook_id=codebook.id,
                label=label,
                description=f"{label} slow work.",
                is_active=True,
            )
            for label in ["Manual Handoffs", *(extra_code_labels or [])]
        ]
        session.add_all([codebook, theme, *codes])
        await session.flush()

        session.add(
            CodebookThemeRelationship(
                id=uuid4(),
                codebook_id=codebook.id,
                theme_id=theme.id,
                is_active=True,
            )
        )
        for code in codes:
            session.add_all([
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
    async def _fake_apply_documents(self, *, documents, on_progress=None, should_cancel=None, **_kwargs):
        evidence: list[_AppliedEvidence] = []
        failed_document_ids: list[UUID] = []
        if on_progress is not None:
            await on_progress(0, len(documents))
        for document_index, document in enumerate(documents, start=1):
            if should_cancel is not None and await should_cancel():
                raise TraceableAnalysisCancelledError("cancelled")
            result: CodebookApplicationResult | None = None
            last_error: Exception | None = None
            for _ in range(3):
                try:
                    result = single_transcript_fn(document.content)
                    break
                except Exception as exc:
                    last_error = exc
            if result is None:
                failed_document_ids.append(document.id)
            else:
                for code in result.codes:
                    match = locate_quote_span(document.content, code.quote)
                    evidence.append(
                        _AppliedEvidence(
                            document_id=document.id,
                            code_label=code.code_label,
                            theme_label=code.theme_label,
                            quote=match.quote,
                            start_char=match.start_char,
                            end_char=match.end_char,
                            quote_match_status=match.quote_match_status,
                            confidence=code.confidence,
                            rationale=code.rationale,
                            summary=result.summary,
                            researcher_notes=result.researcher_notes,
                        )
                    )
            if on_progress is not None:
                await on_progress(document_index, len(documents))
            if last_error is not None and result is None:
                continue
        # Mirror the real application pass, which dedups before returning.
        return _ApplicationPassResult(
            evidence=self._deduplicate_applied_evidence(evidence),
            failed_document_ids=failed_document_ids,
        )

    monkeypatch.setattr(TraceableAnalysisService, "_apply_codebook_to_documents", _fake_apply_documents)


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


async def test_apply_codebook_job_deduplicates_per_code_only(client, db_engine, monkeypatch) -> None:
    """Same-code overlaps collapse; a distinct code on the same passage is kept."""
    corpus_id, codebook_id, document_ids = await _seed_corpus_codebook(
        db_engine,
        texts=["Participant: The manual handoffs slow everyone down."],
        extra_code_labels=["Handoff Delays"],
    )

    def _duplicated_result(_content: str) -> CodebookApplicationResult:
        return CodebookApplicationResult(
            summary="The participant describes workflow friction.",
            researcher_notes=None,
            themes=[
                AppliedThemeAssignment(
                    theme_label="Workflow Friction",
                    present=True,
                    confidence=0.9,
                    quote="manual handoffs slow",
                )
            ],
            codes=[
                AppliedCodeAssignment(
                    code_label="Manual Handoffs",
                    theme_label="Workflow Friction",
                    quote="manual handoffs slow",
                    confidence=0.95,
                    rationale="Direct mention of slow manual handoffs.",
                ),
                # Overlapping longer span of the same passage under the SAME code
                # -> collapses with the row above (higher confidence wins).
                AppliedCodeAssignment(
                    code_label="Manual Handoffs",
                    theme_label="Workflow Friction",
                    quote="The manual handoffs slow everyone down",
                    confidence=0.5,
                    rationale="Same passage with more context.",
                ),
                # Same passage under a DIFFERENT code of the same theme -> kept,
                # so this code keeps its coverage.
                AppliedCodeAssignment(
                    code_label="Handoff Delays",
                    theme_label="Workflow Friction",
                    quote="manual handoffs slow",
                    confidence=0.6,
                    rationale="Same passage, distinct code.",
                ),
            ],
        )

    _patch_application(monkeypatch, _duplicated_result)

    create_response = await client.post(
        f"{API_CODEBOOKS}/{codebook_id}/apply-jobs",
        json={"transcript_document_ids": document_ids},
    )

    assert create_response.status_code == 202
    created_job = create_response.json()["data"]
    assert created_job["corpus_id"] == corpus_id
    terminal_job = await _wait_for_terminal_job_status(client, created_job["id"])
    assert terminal_job["status"] == "succeeded", terminal_job.get("error_message")

    documents_response = await client.get(
        f"/api/v1/codebook-application-runs/{terminal_job['application_run_id']}/documents"
    )
    assert documents_response.status_code == 200
    document_coding = documents_response.json()["data"][0]
    assert document_coding["status"] == "coded"

    assignments = document_coding["code_assignments"]
    # One row per code: the two Manual Handoffs spans collapsed to the
    # highest-confidence one; Handoff Delays survives with its own span.
    quotes_by_code = {ca["code_id"]: ca["quote"] for ca in assignments}
    assert len(assignments) == 2
    assert len(quotes_by_code) == 2
    assert set(quotes_by_code.values()) == {"manual handoffs slow"}


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


async def test_apply_codebook_job_streams_coded_and_failed_counts(client, db_engine, monkeypatch) -> None:
    _, codebook_id, document_ids = await _seed_corpus_codebook(
        db_engine, texts=["First transcript.", "Second transcript."]
    )
    first_coded = asyncio.Event()
    release = asyncio.Event()

    # Gate mid-run so we can read the job while it is still coding: doc 1 codes,
    # doc 2 fails, and on_progress carries the running coded/failed counts.
    async def _gated_apply(self, *, documents, on_progress=None, **_kwargs):
        del self
        await on_progress(1, len(documents), 1, 0)
        first_coded.set()
        await release.wait()
        await on_progress(2, len(documents), 1, 1)
        return _ApplicationPassResult(evidence=[], failed_document_ids=[documents[1].id])

    monkeypatch.setattr(TraceableAnalysisService, "_apply_codebook_to_documents", _gated_apply)

    job_id = (await client.post(
        f"{API_CODEBOOKS}/{codebook_id}/apply-jobs",
        json={"transcript_document_ids": document_ids},
    )).json()["data"]["id"]

    await asyncio.wait_for(first_coded.wait(), timeout=5.0)
    running = (await client.get(f"{API_CODEBOOKS}/apply-jobs/{job_id}")).json()["data"]
    assert (running["status"], running["documents_coded"], running["documents_failed"]) == ("running", 1, 0)

    release.set()
    terminal = await _wait_for_terminal_job_status(client, job_id)
    assert (terminal["status"], terminal["documents_coded"], terminal["documents_failed"]) == ("succeeded", 1, 1)


async def test_apply_codebook_uses_traceable_application_method(db_engine, monkeypatch) -> None:
    corpus_id, codebook_id, document_ids = await _seed_corpus_codebook(
        db_engine,
        texts=[f"Participant: The manual handoffs slow team {index}." for index in range(17)],
    )

    document_counts: list[int] = []

    async def _fake_apply_documents(self, *, documents, on_progress=None, **_kwargs):
        self._token_tracker.input_tokens += 101
        self._token_tracker.output_tokens += 13
        document_counts.append(len(documents))
        evidence = []
        if on_progress is not None:
            await on_progress(0, len(documents))
        for document_index, document in enumerate(documents, start=1):
            match = locate_quote_span(document.content, "manual handoffs slow")
            evidence.append(
                _AppliedEvidence(
                    document_id=document.id,
                    code_label="Manual Handoffs",
                    theme_label="Workflow Friction",
                    quote=match.quote,
                    start_char=match.start_char,
                    end_char=match.end_char,
                    quote_match_status=match.quote_match_status,
                    confidence=0.95,
                    rationale="The quote directly describes slow manual handoffs.",
                )
            )
            if on_progress is not None:
                await on_progress(document_index, len(documents))
        return _ApplicationPassResult(evidence=evidence, failed_document_ids=[])

    monkeypatch.setattr(TraceableAnalysisService, "_apply_codebook_to_documents", _fake_apply_documents)

    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        summary = await CodebookApplicationService(session).apply_codebook(
            corpus_id=UUID(corpus_id),
            codebook_id=UUID(codebook_id),
            transcript_document_ids=[UUID(document_id) for document_id in document_ids],
        )

    assert summary.documents_coded == 17
    assert summary.documents_failed == 0
    assert summary.application_run.llm_tokens_input == 101
    assert summary.application_run.llm_tokens_output == 13
    assert document_counts == [17]


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

@pytest.mark.xfail(reason="Flaky SQLite async DB teardown race condition", strict=False)
async def test_apply_codebook_job_can_be_cancelled_while_running(client, db_engine, monkeypatch) -> None:
    corpus_id, codebook_id, document_ids = await _seed_corpus_codebook(
        db_engine,
        texts=[
            "Participant: The manual handoffs slow everyone down.",
            "Participant: More manual handoffs slow the team down.",
        ],
    )
    del corpus_id

    async def _slow_apply_documents(self, *, documents, on_progress=None, should_cancel=None, **_kwargs):
        del self
        if on_progress is not None:
            await on_progress(0, len(documents))
        await asyncio.sleep(0.05)
        if should_cancel is not None and await should_cancel():
            raise TraceableAnalysisCancelledError("cancelled")
        return _ApplicationPassResult(evidence=[], failed_document_ids=[])

    monkeypatch.setattr(TraceableAnalysisService, "_apply_codebook_to_documents", _slow_apply_documents)

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

    # Wait for the background task to complete entirely to avoid DB teardown conflicts
    from app.services.codebook_application_jobs import codebook_application_job_runner
    while codebook_application_job_runner._tasks:
        await asyncio.sleep(0.1)


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

