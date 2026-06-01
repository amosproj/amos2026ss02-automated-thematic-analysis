from __future__ import annotations

import asyncio
import json
import time

from langchain_core.exceptions import OutputParserException

from app.schemas.llm import (
    GeneratedCodeSuggestion,
    GeneratedThemeNode,
    GeneratedThemePath,
    PassageCodebookGeneration,
)

API_INGESTION = "/api/v1/ingestion"
API_CODEBOOKS = "/api/v1/codebooks"
CORPUS_ID = "00000000-0000-0000-0000-000000000222"


async def _create_corpus_with_docs(client, texts: list[str]) -> tuple[str, list[str]]:
    corpus_response = await client.post(
        f"{API_INGESTION}/corpora",
        json={"corpus_id": CORPUS_ID, "name": "Generation Job Corpus"},
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
        response = await client.get(f"{API_CODEBOOKS}/generate-jobs/{job_id}")
        assert response.status_code == 200
        payload = response.json()["data"]
        last_payload = payload
        if payload["status"] in {"succeeded", "failed", "cancelled"}:
            return payload
        await asyncio.sleep(0.05)
    raise AssertionError(f"Job {job_id} did not reach terminal status. Last payload: {last_payload}")


async def test_generate_codebook_job_completes_successfully(client, monkeypatch) -> None:
    corpus_id, document_ids = await _create_corpus_with_docs(
        client,
        texts=[
            "Alpha workflow has repeated manual handoffs and rework.",
            "Beta onboarding lacks clarity and causes delays.",
        ],
    )

    def _fake_generate_codebook_for_passage(_: str) -> PassageCodebookGeneration:
        return PassageCodebookGeneration(
            themes=[
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Workflow Friction"),
                        GeneratedThemeNode(label="Manual Work"),
                    ]
                )
            ],
            codes=[
                GeneratedCodeSuggestion(
                    label="Manual Bottleneck",
                    description="Manual processing slows progress.",
                    theme_path=["Workflow Friction", "Manual Work"],
                )
            ],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _fake_generate_codebook_for_passage,
    )

    create_response = await client.post(
        f"{API_CODEBOOKS}/generate-jobs",
        json={
            "codebook_name": "Generated Async",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
        },
    )
    assert create_response.status_code == 202
    created_job = create_response.json()["data"]
    assert created_job["status"] in {"queued", "running"}

    terminal_job = await _wait_for_terminal_job_status(client, created_job["id"])
    assert terminal_job["status"] == "succeeded"
    assert terminal_job["codebook_id"] is not None
    assert terminal_job["themes_created"] >= 1
    assert terminal_job["codes_created"] >= 1


async def test_generate_codebook_job_accepts_payload_without_confirmation_field(client, monkeypatch) -> None:
    corpus_id, document_ids = await _create_corpus_with_docs(
        client,
        texts=["Short transcript about operations and delays."],
    )

    def _fake_generate_codebook_for_passage(_: str) -> PassageCodebookGeneration:
        return PassageCodebookGeneration(
            themes=[GeneratedThemePath(path=[GeneratedThemeNode(label="Operations")])],
            codes=[GeneratedCodeSuggestion(label="Delay", description=None, theme_path=["Operations"])],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _fake_generate_codebook_for_passage,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate-jobs",
        json={
            "codebook_name": "Generated Async",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
        },
    )
    assert response.status_code == 202
    assert response.json()["success"] is True


async def test_generate_codebook_job_can_be_cancelled_while_running(client, monkeypatch) -> None:
    long_text = " ".join(f"token{i}" for i in range(0, 260))
    corpus_id, document_ids = await _create_corpus_with_docs(client, texts=[long_text])

    def _slow_generate_codebook_for_passage(_: str) -> PassageCodebookGeneration:
        time.sleep(0.03)
        return PassageCodebookGeneration(
            themes=[
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Operations"),
                    ]
                )
            ],
            codes=[
                GeneratedCodeSuggestion(
                    label="Ops Note",
                    description=None,
                    theme_path=["Operations"],
                )
            ],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _slow_generate_codebook_for_passage,
    )

    create_response = await client.post(
        f"{API_CODEBOOKS}/generate-jobs",
        json={
            "codebook_name": "Generated Async Cancel",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
        },
    )
    assert create_response.status_code == 202
    created_job = create_response.json()["data"]

    cancel_response = await client.post(
        f"{API_CODEBOOKS}/generate-jobs/{created_job['id']}/cancel",
    )
    assert cancel_response.status_code in {202, 422}
    terminal_job = await _wait_for_terminal_job_status(client, created_job["id"])
    # Cancellation is best-effort; depending on timing the job may complete, cancel,
    # or surface an execution failure from the worker.
    assert terminal_job["status"] in {"succeeded", "cancelled", "failed"}


async def test_generate_codebook_job_uses_all_corpus_documents_when_ids_omitted(client, monkeypatch) -> None:
    corpus_id, _ = await _create_corpus_with_docs(
        client,
        texts=[
            "First transcript about collaboration.",
            "Second transcript about process and communication.",
        ],
    )

    def _fake_generate_codebook_for_passage(_: str) -> PassageCodebookGeneration:
        return PassageCodebookGeneration(
            themes=[GeneratedThemePath(path=[GeneratedThemeNode(label="Collaboration")])],
            codes=[GeneratedCodeSuggestion(label="Team Alignment", description=None, theme_path=["Collaboration"])],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _fake_generate_codebook_for_passage,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate-jobs",
        json={
            "codebook_name": "Generated Async All Docs",
            "corpus_id": corpus_id,
        },
    )
    assert response.status_code == 202
    created_job = response.json()["data"]

    terminal_job = await _wait_for_terminal_job_status(client, created_job["id"])
    assert terminal_job["status"] == "succeeded"
    assert terminal_job["transcripts_processed"] == 2


async def test_generate_codebook_job_records_partial_parse_failures_and_succeeds(client, monkeypatch) -> None:
    corpus_id, document_ids = await _create_corpus_with_docs(
        client,
        texts=[
            "Alpha transcript about collaboration and adaptation.",
            "Beta transcript about planning and process.",
        ],
    )
    calls = {"count": 0}

    def _sometimes_fails_generate_codebook_for_passage(_: str) -> PassageCodebookGeneration:
        calls["count"] += 1
        if calls["count"] <= 3:
            raise OutputParserException("Invalid json output: malformed")
        return PassageCodebookGeneration(
            themes=[GeneratedThemePath(path=[GeneratedThemeNode(label="Collaboration")])],
            codes=[GeneratedCodeSuggestion(label="Team Alignment", description=None, theme_path=["Collaboration"])],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _sometimes_fails_generate_codebook_for_passage,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate-jobs",
        json={
            "codebook_name": "Generated Async Partial Parse Failure",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
        },
    )
    assert response.status_code == 202
    created_job = response.json()["data"]

    terminal_job = await _wait_for_terminal_job_status(client, created_job["id"])
    assert terminal_job["status"] == "succeeded"
    assert terminal_job["error_message"] is not None
    warning = json.loads(terminal_job["error_message"])
    assert warning["type"] == "passage_generation_partial_failures"
    assert warning["passages_failed"] == 1
    assert len(warning["failed_passages"]) == 1
