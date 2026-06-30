from __future__ import annotations

import asyncio
import time

from app.schemas.traceable_llm import (
    CodebookQualityEvaluationResult,
    CodebookSynthesisResult,
    SynthesizedCode,
    SynthesizedThemeNode,
    SynthesizedThemePath,
)
from app.services.traceable_analysis import _ApplicationPassResult, _AppliedEvidence, _QuoteEvidence
from app.services.traceable_code_consolidation import ConsolidatedCode

API_INGESTION = "/api/v1/ingestion"
API_CODEBOOKS = "/api/v1/codebooks"
CORPUS_ID = "00000000-0000-0000-0000-000000000333"


async def _create_corpus_with_docs(client, texts: list[str]) -> tuple[str, list[str]]:
    corpus_response = await client.post(
        f"{API_INGESTION}/corpora",
        json={"corpus_id": CORPUS_ID, "name": "Traceable Analysis Corpus"},
    )
    assert corpus_response.status_code == 201
    corpus_id = corpus_response.json()["data"]["id"]

    ingest_response = await client.post(
        f"{API_INGESTION}/corpora/{corpus_id}/documents/bulk",
        json={
            "documents": [
                {"title": f"Doc {idx}", "text": text}
                for idx, text in enumerate(texts, start=1)
            ]
        },
    )
    assert ingest_response.status_code == 201

    document_response = await client.get(f"{API_INGESTION}/corpora/{corpus_id}/documents")
    assert document_response.status_code == 200
    document_ids = [row["id"] for row in document_response.json()["data"]["items"]]
    return corpus_id, document_ids


async def _wait_for_terminal_job_status(client, job_id: str, timeout_seconds: float = 10.0) -> dict:
    started = time.monotonic()
    last_payload: dict = {}
    while time.monotonic() - started < timeout_seconds:
        response = await client.get(f"{API_CODEBOOKS}/generate-apply-jobs/{job_id}")
        assert response.status_code == 200
        payload = response.json()["data"]
        last_payload = payload
        if payload["status"] in {"succeeded", "failed", "cancelled"}:
            return payload
        await asyncio.sleep(0.25)
    raise AssertionError(f"Job {job_id} did not reach terminal status. Last payload: {last_payload}")


async def test_traceable_analysis_job_creates_codebook_and_application_run(client, monkeypatch) -> None:
    corpus_id, document_ids = await _create_corpus_with_docs(
        client,
        texts=["The project has manual handoffs slow every review and creates rework."],
    )

    async def _fake_extract_quote_codes(self, *, documents, **_kwargs):
        document = documents[0]
        quote = "manual handoffs slow"
        start = document.content.index(quote)
        return [
            _QuoteEvidence(
                quote_id="quote-1",
                document_id=document.id,
                quote=quote,
                start_char=start,
                end_char=start + len(quote),
                quote_match_status="exact",
                candidate_id="candidate-1",
                code_label="Manual handoffs slow work",
                code_description="Manual handoffs slow the review workflow.",
                confidence=0.93,
                rationale="The quote directly names slow manual handoffs.",
            )
        ]

    async def _fake_consolidate(candidates, **_kwargs):
        return [
            ConsolidatedCode(
                label="Manual handoffs slow work",
                description="Manual handoffs slow the review workflow.",
                candidate_ids=[candidate.candidate_id for candidate in candidates],
                quote_ids=[quote_id for candidate in candidates for quote_id in candidate.quote_ids],
            )
        ], []

    async def _fake_synthesize(self, *, consolidated_codes, **_kwargs):
        code = consolidated_codes[0]
        return CodebookSynthesisResult(
            themes=[
                SynthesizedThemePath(
                    path=[
                        SynthesizedThemeNode(
                            label="Workflow Friction",
                            description="Recurring friction in the work process.",
                        )
                    ]
                )
            ],
            codes=[
                SynthesizedCode(
                    code_label=code.label,
                    code_description=code.description,
                    theme_path=["Workflow Friction"],
                )
            ],
        )

    async def _fake_apply_codebook_to_documents(self, *, documents, **_kwargs):
        document = documents[0]
        quote = "manual handoffs slow"
        start = document.content.index(quote)
        return _ApplicationPassResult(
            evidence=[
                _AppliedEvidence(
                    document_id=document.id,
                    code_label="Manual handoffs slow work",
                    theme_label="Workflow Friction",
                    quote=quote,
                    start_char=start,
                    end_char=start + len(quote),
                    quote_match_status="exact",
                    confidence=0.91,
                    rationale="The generated codebook code is directly supported.",
                )
            ],
            failed_document_ids=[],
        )

    async def _fake_evaluate_codebook_quality(self, **_kwargs):
        return CodebookQualityEvaluationResult(
            fitness_score=0.95,
            coverage_score=0.95,
            notes="Offline test evaluator.",
        )

    async def _fake_polish_final_codebook(self, *, synthesis, consolidated_codes, quote_evidence):
        return synthesis, consolidated_codes, quote_evidence, []

    monkeypatch.setattr(
        "app.services.traceable_analysis.TraceableAnalysisService._extract_quote_codes",
        _fake_extract_quote_codes,
    )
    monkeypatch.setattr(
        "app.services.traceable_analysis.consolidate_code_candidates",
        _fake_consolidate,
    )
    monkeypatch.setattr(
        "app.services.traceable_analysis.TraceableAnalysisService._synthesize_codebook",
        _fake_synthesize,
    )
    monkeypatch.setattr(
        "app.services.traceable_analysis.TraceableAnalysisService._apply_codebook_to_documents",
        _fake_apply_codebook_to_documents,
    )
    monkeypatch.setattr(
        "app.services.traceable_analysis.TraceableAnalysisService._evaluate_codebook_quality",
        _fake_evaluate_codebook_quality,
    )
    monkeypatch.setattr(
        "app.services.traceable_analysis.TraceableAnalysisService._polish_final_codebook",
        _fake_polish_final_codebook,
    )

    create_response = await client.post(
        f"{API_CODEBOOKS}/generate-apply-jobs",
        json={
            "codebook_name": "Traceable Generated",
            "analysis_name": "Traceable Application",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
            "research_query": "How do participants describe workflow friction?",
            "max_refinement_rounds": 0,
        },
    )
    assert create_response.status_code == 202
    job_id = create_response.json()["data"]["id"]

    job = await _wait_for_terminal_job_status(client, job_id)
    assert job["status"] == "succeeded"
    assert job["codebook_id"]
    assert job["application_run_id"]
    assert job["quotes_created"] == 1
    assert job["codes_created"] == 1
    assert job["themes_created"] == 1

    codebook_response = await client.get(f"{API_CODEBOOKS}/{job['codebook_id']}")
    assert codebook_response.status_code == 200
    codebook = codebook_response.json()["data"]
    assert codebook["name"] == "Traceable Generated"
    assert codebook["themes"][0]["name"] == "Workflow Friction"
    assert codebook["codes"][0]["name"] == "Manual handoffs slow work"

    run_response = await client.get(f"/api/v1/codebook-application-runs/{job['application_run_id']}")
    assert run_response.status_code == 200
    run = run_response.json()["data"]
    assert run["status"] == "succeeded"
    assert run["document_codings"][0]["code_assignments"][0]["quote"] == "manual handoffs slow"
    assert run["document_codings"][0]["code_assignments"][0]["quote_match_status"] == "exact"
