from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import (
    Code,
    Codebook,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Theme,
    ThemeHierarchyRelationship,
)
from app.schemas.llm import (
    GeneratedCodeSuggestion,
    GeneratedThemeNode,
    GeneratedThemePath,
    PassageCodebookGeneration,
)
from langchain_core.exceptions import OutputParserException

API_INGESTION = "/api/v1/ingestion"
API_CODEBOOKS = "/api/v1/codebooks"
PROJECT_ID = "00000000-0000-0000-0000-000000000111"


async def _create_corpus_and_docs(client) -> tuple[str, list[str]]:
    corpus_response = await client.post(
        f"{API_INGESTION}/corpora",
        json={"project_id": PROJECT_ID, "name": "Generation Corpus"},
    )
    assert corpus_response.status_code == 201
    corpus_id = corpus_response.json()["data"]["id"]

    ingest_response = await client.post(
        f"{API_INGESTION}/corpora/{corpus_id}/documents/bulk",
        json={
            "documents": [
                {"title": "Doc Alpha", "text": "Alpha passage with repeated manual work and bottlenecks."},
                {"title": "Doc Beta", "text": "Beta passage about onboarding and training gaps."},
            ]
        },
    )
    assert ingest_response.status_code == 201

    document_response = await client.get(f"{API_INGESTION}/corpora/{corpus_id}/documents")
    assert document_response.status_code == 200
    document_ids = [row["id"] for row in document_response.json()["data"]["items"]]
    return corpus_id, document_ids


async def test_generate_codebook_creates_deduplicated_themes_and_codes(
    client,
    db_engine,
    monkeypatch,
) -> None:
    corpus_id, document_ids = await _create_corpus_and_docs(client)

    def _fake_generate_codebook_for_passage(passage: str) -> PassageCodebookGeneration:
        if "Alpha" in passage:
            return PassageCodebookGeneration(
                themes=[
                    GeneratedThemePath(
                        path=[
                            GeneratedThemeNode(
                                label="Workflow Friction",
                                description="Repeated process inefficiencies.",
                            ),
                            GeneratedThemeNode(
                                label="Manual Work",
                                description="Reliance on manual steps.",
                            ),
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

        return PassageCodebookGeneration(
            themes=[
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Workflow Friction"),
                        GeneratedThemeNode(label="Manual Work"),
                    ]
                ),
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Workflow Friction"),
                        GeneratedThemeNode(label="Training Gaps"),
                    ]
                ),
            ],
            codes=[
                GeneratedCodeSuggestion(
                    label="manual bottleneck",
                    description="Duplicate label with different casing.",
                    theme_path=["Workflow Friction", "Manual Work"],
                ),
                GeneratedCodeSuggestion(
                    label="Onboarding Unclear",
                    description="Insufficient onboarding documentation.",
                    theme_path=["Workflow Friction", "Training Gaps"],
                ),
            ],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _fake_generate_codebook_for_passage,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Generated v1",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
        },
    )
    assert response.status_code == 201

    payload = response.json()["data"]
    assert payload["transcripts_processed"] == 2
    assert payload["themes_created"] == 3
    assert payload["codes_created"] == 2

    codebook_id = UUID(payload["codebook"]["id"])
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        codebook = (
            await session.execute(select(Codebook).where(Codebook.id == codebook_id))
        ).scalar_one_or_none()
        assert codebook is not None
        assert codebook.name == "Generated v1"

        theme_rows = list(
            (
                await session.scalars(
                    select(Theme)
                    .join(
                        CodebookThemeRelationship,
                        and_(
                            CodebookThemeRelationship.theme_id == Theme.id,
                            CodebookThemeRelationship.codebook_id == codebook.id,
                        ),
                    )
                )
            ).all()
        )
        assert len(theme_rows) == 3
        assert sorted(theme.label for theme in theme_rows) == [
            "Manual Work",
            "Training Gaps",
            "Workflow Friction",
        ]

        hierarchy_rows = list(
            (
                await session.scalars(
                    select(ThemeHierarchyRelationship).where(
                        ThemeHierarchyRelationship.codebook_id == codebook.id
                    )
                )
            ).all()
        )
        assert len(hierarchy_rows) == 2

        code_rows = list(
            (
                await session.scalars(
                    select(Code)
                    .join(
                        CodebookCodeRelationship,
                        and_(
                            CodebookCodeRelationship.code_id == Code.id,
                            CodebookCodeRelationship.codebook_id == codebook.id,
                        ),
                    )
                )
            ).all()
        )
        assert len(code_rows) == 2
        assert sorted(code.label for code in code_rows) == [
            "Manual Bottleneck",
            "Onboarding Unclear",
        ]


async def test_generate_codebook_rejects_documents_outside_selected_corpus(
    client,
    monkeypatch,
) -> None:
    corpus_a_id, _ = await _create_corpus_and_docs(client)
    corpus_b_id, corpus_b_document_ids = await _create_corpus_and_docs(client)

    def _never_called(_: str) -> PassageCodebookGeneration:
        raise AssertionError("LLM generation should not be called for invalid transcript selection")

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _never_called,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Invalid Selection",
            "corpus_id": corpus_a_id,
            "transcript_document_ids": corpus_b_document_ids,
        },
    )
    assert response.status_code == 422
    assert response.json()["success"] is False
    assert "not found in the selected corpus" in response.json()["error"]

    # Control assertion: the second corpus genuinely exists and has docs, so failure
    # was due to corpus mismatch and not missing records.
    assert corpus_b_id != corpus_a_id


async def test_generate_codebook_returns_404_for_unknown_corpus(client) -> None:
    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Unknown Corpus",
            "corpus_id": str(uuid4()),
            "transcript_document_ids": [str(uuid4())],
        },
    )
    assert response.status_code == 404
    assert response.json()["success"] is False


async def test_generate_codebook_uses_all_corpus_documents_when_ids_omitted(
    client,
    monkeypatch,
) -> None:
    corpus_id, _ = await _create_corpus_and_docs(client)

    def _fake_generate_codebook_for_passage(_: str) -> PassageCodebookGeneration:
        return PassageCodebookGeneration(
            themes=[
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Workflow Friction"),
                    ]
                )
            ],
            codes=[
                GeneratedCodeSuggestion(
                    label="Process Delay",
                    description=None,
                    theme_path=["Workflow Friction"],
                )
            ],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _fake_generate_codebook_for_passage,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "All Docs",
            "corpus_id": corpus_id,
        },
    )
    assert response.status_code == 201
    payload = response.json()["data"]
    assert payload["transcripts_processed"] == 2


async def test_generate_codebook_merges_duplicate_theme_labels_across_paths(
    client,
    db_engine,
    monkeypatch,
) -> None:
    corpus_id, document_ids = await _create_corpus_and_docs(client)

    def _fake_generate_codebook_for_passage(passage: str) -> PassageCodebookGeneration:
        if "Alpha" in passage:
            return PassageCodebookGeneration(
                themes=[
                    GeneratedThemePath(
                        path=[
                            GeneratedThemeNode(label="Planning"),
                            GeneratedThemeNode(label="Future Expectations"),
                        ]
                    )
                ],
                codes=[],
            )
        return PassageCodebookGeneration(
            themes=[
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Adoption"),
                        GeneratedThemeNode(label="Future Expectations"),
                    ]
                )
            ],
            codes=[],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _fake_generate_codebook_for_passage,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Duplicate Labels",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
        },
    )
    assert response.status_code == 201
    payload = response.json()["data"]
    assert payload["themes_created"] == 3


async def test_generate_codebook_continues_when_one_passage_has_output_parse_error(
    client,
    monkeypatch,
) -> None:
    corpus_id, document_ids = await _create_corpus_and_docs(client)
    calls = {"count": 0}

    def _sometimes_fails_generate_codebook_for_passage(_: str) -> PassageCodebookGeneration:
        calls["count"] += 1
        if calls["count"] <= 3:
            raise OutputParserException("Invalid json output: malformed")
        return PassageCodebookGeneration(
            themes=[
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Workflow Friction"),
                    ]
                )
            ],
            codes=[
                GeneratedCodeSuggestion(
                    label="Process Delay",
                    description=None,
                    theme_path=["Workflow Friction"],
                )
            ],
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _sometimes_fails_generate_codebook_for_passage,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Partial Parse Failure",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
        },
    )
    assert response.status_code == 201
    payload = response.json()["data"]
    assert payload["passages_failed"] == 1
    assert len(payload["failed_passages"]) == 1
    assert payload["failed_passages"][0]["passage_index"] == 1
