import uuid

import pytest
import pytest_asyncio

from app.domain.enums import DocumentStatus, IngestionRunStatus, SourceType
from app.exceptions import NotFoundError
from app.schemas.ingestion import CorpusCreate, DocumentInput
from app.services.ingestion import IngestionService

P1 = uuid.UUID("00000000-0000-0000-0000-000000000001")
P2 = uuid.UUID("00000000-0000-0000-0000-000000000002")


# ---------------------------------------------------------------------------
# Corpus CRUD
# ---------------------------------------------------------------------------


async def test_create_corpus(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(
        CorpusCreate(project_id=P1, name="Test Corpus", metadata={"k": "v"})
    )
    assert corpus.id
    assert corpus.project_id == P1
    assert corpus.name == "Test Corpus"
    assert corpus.extra_metadata == {"k": "v"}


async def test_get_corpus_not_found_raises(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    with pytest.raises(NotFoundError):
        await svc.get_corpus(uuid.uuid4())


async def test_list_corpora_empty(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpora, total = await svc.list_corpora(project_id=P1)
    assert corpora == []
    assert total == 0


async def test_list_corpora_filters_by_project(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    await svc.create_corpus(CorpusCreate(project_id=P1, name="A"))
    await svc.create_corpus(CorpusCreate(project_id=P2, name="B"))

    result, total = await svc.list_corpora(project_id=P1)
    assert total == 1
    assert result[0].project_id == P1


# ---------------------------------------------------------------------------
# Document ingestion — valid documents
# ---------------------------------------------------------------------------


async def test_ingest_one_valid_document(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text="one two three four five")],
        source_type=SourceType.MANUAL,
    )

    assert result.run.accepted_documents == 1
    assert result.run.rejected_documents == 0
    assert result.run.status == IngestionRunStatus.COMPLETED
    assert len(result.documents) == 1
    assert result.documents[0].status == DocumentStatus.ACTIVE


async def test_ingest_creates_chunks(db_session, test_settings):
    # test_settings: chunk_size=10, overlap=2 → stride=8
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    # 18 words → chunk 0: [0..10), chunk 1: [8..18)
    text = " ".join(str(i) for i in range(18))
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text=text)],
        source_type=SourceType.MANUAL,
    )

    assert result.chunks_created == 2


async def test_chunk_metadata_correct(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    # 15 words, chunk_size=10, overlap=2 → stride=8
    # chunk 0: [0..10), chunk 1: [8..15)
    text = " ".join(str(i) for i in range(15))
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text=text)],
        source_type=SourceType.MANUAL,
    )

    chunks, _ = await svc.list_chunks(corpus_id=corpus.id)
    assert len(chunks) == 2
    assert chunks[0].start_word == 0
    assert chunks[0].end_word == 10
    assert chunks[0].chunk_index == 0
    assert chunks[1].start_word == 8
    assert chunks[1].end_word == 15
    assert chunks[1].chunk_index == 1


# ---------------------------------------------------------------------------
# Document ingestion — rejected / empty / duplicate
# ---------------------------------------------------------------------------


async def test_empty_document_is_rejected(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text="")],
        source_type=SourceType.MANUAL,
    )

    assert result.run.empty_documents == 1
    assert result.run.rejected_documents == 1
    assert result.run.accepted_documents == 0
    assert len(result.documents) == 0


async def test_whitespace_only_document_is_rejected(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text="   \n\t  ")],
        source_type=SourceType.MANUAL,
    )

    assert result.run.empty_documents == 1
    assert result.run.rejected_documents == 1


async def test_oversized_document_is_rejected(db_session, test_settings):
    # test_settings: max=50 words
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    big_text = " ".join(["word"] * 60)
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text=big_text)],
        source_type=SourceType.MANUAL,
    )

    assert result.run.rejected_documents == 1
    assert result.run.accepted_documents == 0
    assert result.run.empty_documents == 0


async def test_duplicate_document_is_skipped(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    doc = DocumentInput(text="unique text for dedup test")

    await svc.ingest_documents(
        corpus_id=corpus.id, documents=[doc], source_type=SourceType.MANUAL
    )
    result = await svc.ingest_documents(
        corpus_id=corpus.id, documents=[doc], source_type=SourceType.MANUAL
    )

    assert result.run.duplicate_documents == 1
    assert result.run.accepted_documents == 0


async def test_deduplication_disabled_allows_same_hash(db_session):
    from app.config import Settings

    settings = Settings(
        DATABASE_URL="sqlite+aiosqlite:///:memory:",
        INGESTION_CHUNK_SIZE_WORDS=10,
        INGESTION_CHUNK_OVERLAP_WORDS=2,
        INGESTION_MAX_DOCUMENT_WORDS=50,
        INGESTION_DEDUPLICATE_BY_HASH=False,
    )
    svc = IngestionService(db_session, settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    doc = DocumentInput(text="same text document")

    await svc.ingest_documents(
        corpus_id=corpus.id, documents=[doc], source_type=SourceType.MANUAL
    )
    # Second ingest will fail due to the UniqueConstraint on (corpus_id, text_hash),
    # but deduplication logic is bypassed — the service should raise UnprocessableError
    from app.exceptions import UnprocessableError

    with pytest.raises(UnprocessableError):
        await svc.ingest_documents(
            corpus_id=corpus.id, documents=[doc], source_type=SourceType.MANUAL
        )


# ---------------------------------------------------------------------------
# total_documents count
# ---------------------------------------------------------------------------


async def test_total_documents_is_sum_of_all(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    docs = [
        DocumentInput(text="valid doc one"),
        DocumentInput(text=""),  # empty → rejected
        DocumentInput(text=" ".join(["w"] * 60)),  # oversized → rejected
    ]
    result = await svc.ingest_documents(
        corpus_id=corpus.id, documents=docs, source_type=SourceType.MANUAL
    )

    assert result.run.total_documents == (
        result.run.accepted_documents
        + result.run.rejected_documents
        + result.run.duplicate_documents
    )


# ---------------------------------------------------------------------------
# list_documents / list_chunks
# ---------------------------------------------------------------------------


async def test_list_documents_paginated(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    docs = [DocumentInput(text=f"Document number {i}") for i in range(5)]
    await svc.ingest_documents(corpus_id=corpus.id, documents=docs, source_type=SourceType.MANUAL)

    page1, total = await svc.list_documents(corpus_id=corpus.id, page=1, page_size=3)
    assert total == 5
    assert len(page1) == 3

    page2, _ = await svc.list_documents(corpus_id=corpus.id, page=2, page_size=3)
    assert len(page2) == 2


async def test_list_chunks_for_corpus(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    text = " ".join(str(i) for i in range(20))  # 20 words → 2 chunks (size=10, overlap=2)
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text=text)],
        source_type=SourceType.MANUAL,
    )

    chunks, total = await svc.list_chunks(corpus_id=corpus.id)
    assert total == result.chunks_created


async def test_list_chunks_filter_by_document(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    text = " ".join(str(i) for i in range(20))
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text=text), DocumentInput(text="short text here now")],
        source_type=SourceType.MANUAL,
    )

    doc_id = result.documents[0].id
    chunks, total = await svc.list_chunks(corpus_id=corpus.id, document_id=doc_id)
    assert all(c.document_id == doc_id for c in chunks)


# ---------------------------------------------------------------------------
# get_ingestion_run
# ---------------------------------------------------------------------------


async def test_get_ingestion_run(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text="some text here")],
        source_type=SourceType.MANUAL,
    )

    run = await svc.get_ingestion_run(result.run.id)
    assert run.id == result.run.id
    assert run.status == IngestionRunStatus.COMPLETED


async def test_get_ingestion_run_not_found(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    with pytest.raises(NotFoundError):
        await svc.get_ingestion_run(uuid.uuid4())
