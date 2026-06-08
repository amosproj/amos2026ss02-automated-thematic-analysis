from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from langchain_core.exceptions import OutputParserException
from pydantic import ValidationError
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import (
    Code,
    Codebook,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Theme,
    ThemeCodeRelationship,
    ThemeHierarchyRelationship,
)
from app.schemas.llm import (
    CodeConsolidationResult,
    GeneratedCodeSuggestion,
    GeneratedThemeNode,
    GeneratedThemePath,
    PassageCodebookGeneration,
    ThemeConsolidationResult,
)
from app.services.codebook_generation import CodebookGenerationService, _CodeDraft, _ThemeNodeDraft

API_INGESTION = "/api/v1/ingestion"
API_CODEBOOKS = "/api/v1/codebooks"


@pytest.fixture(autouse=True)
def _stub_consolidation_calls(monkeypatch):
    def _identity_code_consolidation(codes, *_, **__):
        return CodeConsolidationResult(codes=codes)

    def _identity_theme_consolidation(themes, *_, **__):
        return ThemeConsolidationResult(themes=themes)

    monkeypatch.setattr(
        "app.services.codebook_generation.consolidate_generated_codes",
        _identity_code_consolidation,
    )
    monkeypatch.setattr(
        "app.services.codebook_generation.consolidate_generated_themes",
        _identity_theme_consolidation,
    )


def test_remap_code_parent_keys_uses_best_matching_final_theme() -> None:
    theme_nodes = {
        ("work, employment & economic wellbeing",): _ThemeNodeDraft(
            key=("work, employment & economic wellbeing",),
            label="Work, Employment & Economic Wellbeing",
        ),
        ("work, employment & economic wellbeing", "employment outlook & job security"): _ThemeNodeDraft(
            key=("work, employment & economic wellbeing", "employment outlook & job security"),
            label="Employment Outlook & Job Security",
        ),
        ("ai governance, regulation & policy",): _ThemeNodeDraft(
            key=("ai governance, regulation & policy",),
            label="AI Governance, Regulation & Policy",
        ),
        ("ai governance, regulation & policy", "ai safety & ethics"): _ThemeNodeDraft(
            key=("ai governance, regulation & policy", "ai safety & ethics"),
            label="AI Safety & Ethics",
        ),
    }
    codes = [
        _CodeDraft(
            label="Concerns about AI-driven job displacement and security",
            description=None,
            parent_theme_key=("labor market impacts", "job security"),
        )
    ]

    remapped = CodebookGenerationService._remap_code_parent_keys(codes, theme_nodes=theme_nodes)

    assert remapped[0].parent_theme_key == (
        "work, employment & economic wellbeing",
        "employment outlook & job security",
    )


def test_remap_code_parent_keys_does_not_force_unmatched_codes_to_first_theme() -> None:
    theme_nodes = {
        ("alpha",): _ThemeNodeDraft(key=("alpha",), label="Alpha"),
        ("beta",): _ThemeNodeDraft(key=("beta",), label="Beta"),
    }
    codes = [
        _CodeDraft(
            label="Completely unrelated concept",
            description=None,
            parent_theme_key=("missing",),
        )
    ]

    remapped = CodebookGenerationService._remap_code_parent_keys(codes, theme_nodes=theme_nodes)

    assert remapped[0].parent_theme_key is None


async def _create_corpus_and_docs(client) -> tuple[str, list[str]]:
    corpus_response = await client.post(
        f"{API_INGESTION}/corpora",
        json={"corpus_id": str(uuid4()), "name": "Generation Corpus"},
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

    def _fake_generate_codebook_for_passage(passage: str, **_kwargs) -> PassageCodebookGeneration:
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
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
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

        theme_code_rows = list(
            (
                await session.scalars(
                    select(ThemeCodeRelationship).where(
                        ThemeCodeRelationship.codebook_id == codebook.id
                    )
                )
            ).all()
        )
        assert len(theme_code_rows) == 2
        code_ids = {code.id for code in code_rows}
        theme_ids = {theme.id for theme in theme_rows}
        assert {row.code_id for row in theme_code_rows} == code_ids
        assert {row.theme_id for row in theme_code_rows}.issubset(theme_ids)


async def test_generate_codebook_post_processes_codes_with_llm_consolidation(
    client,
    db_engine,
    monkeypatch,
) -> None:
    corpus_id, document_ids = await _create_corpus_and_docs(client)

    def _fake_generate_codebook_for_passage(_: str, **_kwargs) -> PassageCodebookGeneration:
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
                    label="Manual Delay",
                    description="Manual steps slow work.",
                    theme_path=["Workflow Friction"],
                ),
                GeneratedCodeSuggestion(
                    label="Manual Bottleneck",
                    description="Repeated handoffs create delay.",
                    theme_path=["Workflow Friction"],
                ),
            ],
        )

    def _fake_consolidate_generated_codes(_):
        return CodeConsolidationResult(
            codes=[
                {
                    "label": "Manual Bottleneck",
                    "description": "Consolidated operational delay due to manual work.",
                }
            ]
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _fake_generate_codebook_for_passage,
    )
    monkeypatch.setattr(
        "app.services.codebook_generation.consolidate_generated_codes",
        _fake_consolidate_generated_codes,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Consolidated Codes",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
        },
    )
    assert response.status_code == 201
    payload = response.json()["data"]
    assert payload["codes_created"] == 1

    codebook_id = UUID(payload["codebook"]["id"])
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        code_rows = list(
            (
                await session.scalars(
                    select(Code)
                    .join(
                        CodebookCodeRelationship,
                        and_(
                            CodebookCodeRelationship.code_id == Code.id,
                            CodebookCodeRelationship.codebook_id == codebook_id,
                        ),
                    )
                )
            ).all()
        )
        assert [code.label for code in code_rows] == ["Manual Bottleneck"]

        theme_code_rows = list(
            (
                await session.scalars(
                    select(ThemeCodeRelationship).where(
                        ThemeCodeRelationship.codebook_id == codebook_id
                    )
                )
            ).all()
        )
        assert len(theme_code_rows) == 1
        assert theme_code_rows[0].code_id == code_rows[0].id


async def test_generate_codebook_post_processes_themes_with_llm_consolidation(
    client,
    db_engine,
    monkeypatch,
) -> None:
    corpus_id, document_ids = await _create_corpus_and_docs(client)

    def _fake_generate_codebook_for_passage(_: str, **_kwargs) -> PassageCodebookGeneration:
        return PassageCodebookGeneration(
            themes=[
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Process Quality"),
                        GeneratedThemeNode(label="Manual Steps"),
                    ]
                ),
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Process Quality"),
                        GeneratedThemeNode(label="Hand-off Delays"),
                    ]
                ),
                GeneratedThemePath(
                    path=[
                        GeneratedThemeNode(label="Workflow Reliability"),
                        GeneratedThemeNode(label="Manual Steps"),
                    ]
                ),
            ],
            codes=[
                GeneratedCodeSuggestion(
                    label="Process Delay",
                    description="Delays in operational flow.",
                    theme_path=["Process Quality", "Manual Steps"],
                )
            ],
        )

    def _fake_consolidate_generated_themes(_, *__, **___):
        return ThemeConsolidationResult(
            themes=[
                {
                    "path": [
                        {"label": "Workflow Friction"},
                        {"label": "Manual Work"},
                    ]
                }
            ]
        )

    monkeypatch.setattr(
        "app.services.codebook_generation.generate_codebook_for_passage",
        _fake_generate_codebook_for_passage,
    )
    monkeypatch.setattr(
        "app.services.codebook_generation.consolidate_generated_themes",
        _fake_consolidate_generated_themes,
    )

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Consolidated Themes",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
        },
    )
    assert response.status_code == 201
    payload = response.json()["data"]
    assert payload["themes_created"] == 2

    codebook_id = UUID(payload["codebook"]["id"])
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        theme_rows = list(
            (
                await session.scalars(
                    select(Theme)
                    .join(
                        CodebookThemeRelationship,
                        and_(
                            CodebookThemeRelationship.theme_id == Theme.id,
                            CodebookThemeRelationship.codebook_id == codebook_id,
                        ),
                    )
                )
            ).all()
        )
        assert sorted(theme.label for theme in theme_rows) == ["Manual Work", "Workflow Friction"]

        hierarchy_rows = list(
            (
                await session.scalars(
                    select(ThemeHierarchyRelationship).where(
                        ThemeHierarchyRelationship.codebook_id == codebook_id
                    )
                )
            ).all()
        )
        assert len(hierarchy_rows) == 1

        code_row = (
            await session.scalars(
                select(Code)
                .join(
                    CodebookCodeRelationship,
                    and_(
                        CodebookCodeRelationship.code_id == Code.id,
                        CodebookCodeRelationship.codebook_id == codebook_id,
                    ),
                )
            )
        ).one()
        theme_by_id = {theme.id: theme for theme in theme_rows}
        theme_code_row = (
            await session.scalars(
                select(ThemeCodeRelationship).where(
                    ThemeCodeRelationship.codebook_id == codebook_id
                )
            )
        ).one()
        assert theme_code_row.code_id == code_row.id
        assert theme_by_id[theme_code_row.theme_id].label == "Manual Work"


async def test_generate_codebook_rejects_documents_outside_selected_corpus(
    client,
    monkeypatch,
) -> None:
    corpus_a_id, _ = await _create_corpus_and_docs(client)
    corpus_b_id, corpus_b_document_ids = await _create_corpus_and_docs(client)

    def _never_called(_: str, **_kwargs) -> PassageCodebookGeneration:
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
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
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
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
        },
    )
    assert response.status_code == 404
    assert response.json()["success"] is False


async def test_generate_codebook_uses_all_corpus_documents_when_ids_omitted(
    client,
    monkeypatch,
) -> None:
    corpus_id, _ = await _create_corpus_and_docs(client)

    def _fake_generate_codebook_for_passage(_: str, **_kwargs) -> PassageCodebookGeneration:
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
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
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

    def _fake_generate_codebook_for_passage(passage: str, **_kwargs) -> PassageCodebookGeneration:
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
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
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

    def _sometimes_fails_generate_codebook_for_passage(_: str, **_kwargs) -> PassageCodebookGeneration:
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
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
        },
    )
    assert response.status_code == 201
    payload = response.json()["data"]
    assert payload["passages_failed"] == 1
    assert len(payload["failed_passages"]) == 1
    assert payload["failed_passages"][0]["passage_index"] == 1


async def test_generate_codebook_continues_when_one_passage_has_validation_error(
    client,
    monkeypatch,
) -> None:
    corpus_id, document_ids = await _create_corpus_and_docs(client)
    calls = {"count": 0}

    def _sometimes_fails_generate_codebook_for_passage(_: str, **_kwargs) -> PassageCodebookGeneration:
        calls["count"] += 1
        if calls["count"] <= 3:
            # Simulate malformed LLM payload that fails pydantic schema validation.
            raise ValidationError.from_exception_data(
                "PassageCodebookGeneration",
                [
                    {
                        "type": "missing",
                        "loc": ("codes", 0, "theme_path"),
                        "msg": "Field required",
                        "input": {"label": "bad"},
                    }
                ],
            )
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
            "codebook_name": "Partial Validation Failure",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
            "research_query": "How do participants describe workflow friction and manual bottlenecks?",
        },
    )
    assert response.status_code == 201
    payload = response.json()["data"]
    assert payload["passages_failed"] == 1
    assert len(payload["failed_passages"]) == 1


async def test_generate_codebook_allows_missing_research_query(client, monkeypatch) -> None:
    # research_query is optional: omitting it generates a codebook with no
    # research-focus steering rather than being rejected.
    corpus_id, document_ids = await _create_corpus_and_docs(client)

    def _fake(passage: str, **_kwargs) -> PassageCodebookGeneration:
        return PassageCodebookGeneration(
            themes=[GeneratedThemePath(path=[GeneratedThemeNode(label="Workflow Friction")])],
            codes=[GeneratedCodeSuggestion(label="Delay", description=None, theme_path=["Workflow Friction"])],
        )

    monkeypatch.setattr("app.services.codebook_generation.generate_codebook_for_passage", _fake)

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "No Query",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
        },
    )
    assert response.status_code == 201


async def test_generate_codebook_rejects_too_long_research_query(client) -> None:
    corpus_id, document_ids = await _create_corpus_and_docs(client)
    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Long Query",
            "corpus_id": corpus_id,
            "transcript_document_ids": document_ids,
            "research_query": "a" * 501,
        },
    )
    assert response.status_code == 422


async def test_generate_codebook_persists_research_query_on_codebook(
    client,
    db_engine,
    monkeypatch,
) -> None:
    corpus_id, document_ids = await _create_corpus_and_docs(client)
    query = "How do participants describe workflow friction and manual bottlenecks?"

    def _fake(passage: str, **_kwargs) -> PassageCodebookGeneration:
        return PassageCodebookGeneration(
            themes=[GeneratedThemePath(path=[GeneratedThemeNode(label="Workflow Friction")])],
            codes=[GeneratedCodeSuggestion(label="Delay", description=None, theme_path=["Workflow Friction"])],
        )

    monkeypatch.setattr("app.services.codebook_generation.generate_codebook_for_passage", _fake)

    response = await client.post(
        f"{API_CODEBOOKS}/generate",
        json={
            "codebook_name": "Query Persistence",
            "corpus_id": corpus_id,
            "research_query": query,
        },
    )
    assert response.status_code == 201

    codebook_id = UUID(response.json()["data"]["codebook"]["id"])
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        codebook = (await session.execute(select(Codebook).where(Codebook.id == codebook_id))).scalar_one()
        assert codebook.research_query == query
