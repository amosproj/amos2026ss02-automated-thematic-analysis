from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import Settings
from app.exceptions import UnprocessableError
from app.generation.contracts import (
    CodebookDraft,
    CodeDraft,
    GenerationContext,
    GenerationDocument,
    GenerationInput,
    GenerationResult,
    ThemeDraft,
)
from app.generation.loader import GenerationAlgorithmLoadError, load_generation_algorithm
from app.generation.validation import GenerationDraftValidationError, validate_generation_result
from app.models import Code, Codebook, CodebookGenerationJob, Corpus, CorpusDocument, Theme
from app.services.codebook_generation import CodebookGenerationService
from app.services.codebook_generation_jobs import CodebookGenerationJobRunner


async def _seed_corpus(db_session) -> tuple[UUID, list[UUID]]:
    corpus = Corpus(id=uuid4(), project_id=uuid4(), name="Plugin Corpus")
    document = CorpusDocument(
        id=uuid4(),
        corpus_id=corpus.id,
        title="Transcript 1",
        content="The team describes delays in operational handoffs.",
    )
    db_session.add_all([corpus, document])
    await db_session.commit()
    return corpus.id, [document.id]


def test_default_configuration_loads_traceable_algorithm() -> None:
    settings = Settings(DATABASE_URL="sqlite+aiosqlite:///:memory:")
    loaded = load_generation_algorithm(settings.GENERATION_ALGORITHM)
    assert loaded.spec == "app.generation.algorithms.traceable:create"
    assert loaded.algorithm.algorithm_id == "traceable_analysis"
    assert loaded.algorithm.algorithm_version


def test_valid_local_algorithm_can_be_loaded() -> None:
    loaded = load_generation_algorithm("tests.generation_algorithms.valid:create")
    assert loaded.algorithm.algorithm_id == "deterministic"
    assert loaded.source_sha256 is not None


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("not-a-module", "module.path:factory"),
        ("tests.generation_algorithms.valid:create:extra", "exactly one ':'"),
        ("tests.generation_algorithms.valid:not valid", "not a valid Python identifier"),
    ],
)
def test_malformed_specs_produce_useful_errors(spec: str, expected: str) -> None:
    with pytest.raises(GenerationAlgorithmLoadError, match=expected):
        load_generation_algorithm(spec)


def test_missing_modules_and_factories_produce_useful_errors() -> None:
    with pytest.raises(GenerationAlgorithmLoadError, match="could not be imported"):
        load_generation_algorithm("tests.generation_algorithms.missing:create")
    with pytest.raises(GenerationAlgorithmLoadError, match="has no factory"):
        load_generation_algorithm("tests.generation_algorithms.valid:missing")
    with pytest.raises(GenerationAlgorithmLoadError, match="factory exploded"):
        load_generation_algorithm("tests.generation_algorithms.invalid:create_raises")


@pytest.mark.parametrize(
    "spec",
    [
        "tests.generation_algorithms.invalid:create_missing_generate",
        "tests.generation_algorithms.invalid:create_sync_generate",
        "tests.generation_algorithms.invalid:create_empty_id",
    ],
)
def test_invalid_algorithm_objects_are_rejected(spec: str) -> None:
    with pytest.raises(GenerationAlgorithmLoadError):
        load_generation_algorithm(spec)


async def test_deterministic_algorithm_persists_core_codebook(db_session) -> None:
    corpus_id, document_ids = await _seed_corpus(db_session)
    response = await CodebookGenerationService(db_session).generate_codebook(
        codebook_name="Plugin Generated",
        corpus_id=corpus_id,
        transcript_document_ids=document_ids,
        apply_after_generation=False,
        generation_algorithm="tests.generation_algorithms.valid:create",
        random_seed=123,
    )

    assert response.codebook.name == "Plugin Generated"
    assert response.themes_created == 1
    assert response.codes_created == 1
    assert response.quotes_created == 2

    themes = list((await db_session.scalars(select(Theme))).all())
    codes = list((await db_session.scalars(select(Code))).all())
    assert [theme.label for theme in themes] == ["Operations"]
    assert [code.label for code in codes] == ["Delay"]


async def test_keyword_frequency_research_algorithm_persists_codebook(db_session) -> None:
    corpus_id, document_ids = await _seed_corpus(db_session)
    response = await CodebookGenerationService(db_session).generate_codebook(
        codebook_name="Keyword Frequency Generated",
        corpus_id=corpus_id,
        transcript_document_ids=document_ids,
        research_query="handoffs and delays",
        researcher_topics="operational handoffs",
        apply_after_generation=False,
        generation_algorithm="research_algorithms.keyword_frequency:create",
        random_seed=123,
    )

    assert response.codebook.name == "Keyword Frequency Generated"
    assert response.themes_created == 2
    assert response.codes_created == 5
    assert response.quotes_created == 0
    assert response.provenance["selected_terms"][0] == "handoffs"

    codes = list((await db_session.scalars(select(Code))).all())
    assert "Handoffs" in [code.label for code in codes]


async def test_keyword_frequency_research_algorithm_reports_progress_and_validates() -> None:
    loaded = load_generation_algorithm("research_algorithms.keyword_frequency:create")
    progress: list[tuple[int, int]] = []
    phases: list[str] = []

    async def on_progress(done: int, total: int) -> None:
        progress.append((done, total))

    async def on_phase(phase: str) -> None:
        phases.append(phase)

    result = await loaded.algorithm.generate(
        GenerationInput(
            documents=(
                GenerationDocument(
                    document_id=uuid4(),
                    title="Transcript 1",
                    content="Handoffs create delays, delays affect trust.",
                ),
                GenerationDocument(
                    document_id=uuid4(),
                    title="Transcript 2",
                    content="Operational handoffs need clearer ownership.",
                ),
            ),
            research_query="handoffs and delays",
            researcher_topics="operations",
            random_seed=123,
        ),
        GenerationContext(on_progress=on_progress, on_phase=on_phase),
    )

    validate_generation_result(result, algorithm_id=loaded.algorithm.algorithm_id)
    assert phases == ["keyword_frequency_extracting"]
    assert progress == [(0, 2), (1, 2), (2, 2)]
    assert [theme.label for theme in result.codebook.themes] == [
        "Keyword Patterns",
        "Frequent Terms",
    ]
    assert [code.label for code in result.codebook.codes][:2] == ["Handoffs", "Delays"]


async def test_invalid_draft_fails_without_partial_persistence(db_session) -> None:
    corpus_id, document_ids = await _seed_corpus(db_session)
    with pytest.raises(UnprocessableError, match="duplicates normalized theme label"):
        await CodebookGenerationService(db_session).generate_codebook(
            codebook_name="Invalid Plugin Generated",
            corpus_id=corpus_id,
            transcript_document_ids=document_ids,
            apply_after_generation=False,
            generation_algorithm="tests.generation_algorithms.invalid:create_invalid_draft",
            random_seed=123,
        )

    count = await db_session.scalar(select(func.count()).select_from(Codebook))
    assert count == 0


def test_unknown_parents_and_cycles_are_rejected() -> None:
    unknown_parent = GenerationResult(
        codebook=CodebookDraft(
            themes=(ThemeDraft(key="child", label="Child", parent_theme_key="missing"),),
            codes=(CodeDraft(key="code", label="Code", theme_key="child"),),
        )
    )
    with pytest.raises(GenerationDraftValidationError, match="unknown parent"):
        validate_generation_result(unknown_parent, algorithm_id="test")

    cycle = GenerationResult(
        codebook=CodebookDraft(
            themes=(
                ThemeDraft(key="a", label="A", parent_theme_key="b"),
                ThemeDraft(key="b", label="B", parent_theme_key="a"),
            ),
            codes=(CodeDraft(key="code", label="Code", theme_key="a"),),
        )
    )
    with pytest.raises(GenerationDraftValidationError, match="cycle"):
        validate_generation_result(cycle, algorithm_id="test")


async def test_algorithm_selection_is_captured_when_job_is_created(client, monkeypatch) -> None:
    from app.config import get_settings

    async def _noop_enqueue(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        "app.routers.codebooks.codebook_generation_job_runner.enqueue",
        _noop_enqueue,
    )
    get_settings.cache_clear()
    monkeypatch.setenv("GENERATION_ALGORITHM", "tests.generation_algorithms.valid:create")
    get_settings.cache_clear()

    corpus_response = await client.post(
        "/api/v1/ingestion/corpora",
        json={"corpus_id": str(uuid4()), "name": "Job Plugin Corpus"},
    )
    corpus_id = corpus_response.json()["data"]["id"]
    ingest_response = await client.post(
        f"/api/v1/ingestion/corpora/{corpus_id}/documents/bulk",
        json={"documents": [{"title": "Doc", "text": "Operational delay."}]},
    )
    assert ingest_response.status_code == 201

    create_response = await client.post(
        "/api/v1/codebooks/generate-jobs",
        json={"codebook_name": "Queued Plugin", "corpus_id": corpus_id},
    )
    assert create_response.status_code == 202
    assert create_response.json()["data"]["generation_algorithm"] == (
        "tests.generation_algorithms.valid:create"
    )
    get_settings.cache_clear()


async def test_queued_job_uses_stored_algorithm_selection(db_engine, monkeypatch) -> None:
    from app.config import get_settings

    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with session_factory() as session:
        corpus_id, document_ids = await _seed_corpus(session)
        job = CodebookGenerationJob(
            id=uuid4(),
            status="queued",
            phase="queued",
            codebook_name="Stored Algorithm",
            generation_algorithm="tests.generation_algorithms.valid:create_first",
            corpus_id=corpus_id,
            transcript_document_ids_json=json.dumps([str(document_ids[0])]),
            apply_after_generation=False,
        )
        session.add(job)
        await session.commit()
        job_id = job.id

    monkeypatch.setenv("GENERATION_ALGORITHM", "tests.generation_algorithms.valid:create_second")
    get_settings.cache_clear()
    runner = CodebookGenerationJobRunner()
    await runner._process_one(job_id, session_factory)

    async with session_factory() as session:
        job = await session.get(CodebookGenerationJob, job_id)
        assert job is not None
        assert job.status == "succeeded"
        assert job.codebook_id is not None
        codebook = await session.get(Codebook, job.codebook_id)
        assert codebook is not None
        codes = list(
            (await session.scalars(select(Code).where(Code.codebook_id == codebook.id))).all()
        )
        assert [code.label for code in codes] == ["Delay First"]
    get_settings.cache_clear()


async def test_algorithm_metadata_and_source_hash_are_in_provenance(db_session) -> None:
    corpus_id, document_ids = await _seed_corpus(db_session)
    response = await CodebookGenerationService(db_session).generate_codebook(
        codebook_name="Plugin Provenance",
        corpus_id=corpus_id,
        transcript_document_ids=document_ids,
        apply_after_generation=False,
        generation_algorithm="tests.generation_algorithms.valid:create",
        random_seed=456,
    )

    metadata = response.provenance["generation_algorithm"]
    assert isinstance(metadata, dict)
    assert metadata["module_spec"] == "tests.generation_algorithms.valid:create"
    assert metadata["algorithm_id"] == "deterministic"
    assert metadata["algorithm_version"] == "1.0"
    assert metadata["source_sha256"]
    assert metadata["random_seed"] == 456
    assert metadata["selected_document_ids"] == [str(document_ids[0])]
    assert metadata["token_usage"]["input_tokens"] == 3


def test_builtin_and_research_example_configs_load() -> None:
    assert load_generation_algorithm(
        "app.generation.algorithms.traceable:create"
    ).algorithm.algorithm_id
    assert load_generation_algorithm("research_algorithms.example:create").algorithm.algorithm_id
    assert load_generation_algorithm(
        "research_algorithms.keyword_frequency:create"
    ).algorithm.algorithm_id
