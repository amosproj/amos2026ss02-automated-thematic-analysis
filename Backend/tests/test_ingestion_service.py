import uuid

import pytest

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
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="Test Corpus"))
    assert corpus.id
    assert corpus.project_id == P1
    assert corpus.name == "Test Corpus"


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
# Document ingestion
# ---------------------------------------------------------------------------


async def test_ingest_one_valid_document(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="Doc", text="one two three four five")],
    )

    assert len(result.documents) == 1
    assert result.documents[0].title == "Doc"
    assert result.chunks_created >= 1


async def test_ingest_creates_chunks(db_session, test_settings):
    # test_settings: chunk_size=10, overlap=2 → stride=8
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    # 18 words → chunk 0: [0..10), chunk 1: [8..18)
    text = " ".join(str(i) for i in range(18))
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text=text)],
    )

    assert result.chunks_created == 2


async def test_chunk_order_correct(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    text = " ".join(str(i) for i in range(15))
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text=text)],
    )

    chunks, _ = await svc.list_chunks(corpus_id=corpus.id)
    indices = [c.chunk_index for c in chunks]
    assert indices == list(range(len(chunks)))


async def test_ingest_skips_empty_documents(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="E", text=""), DocumentInput(title="V", text="valid text here")],
    )

    assert len(result.documents) == 1
    assert result.documents[0].title == "V"


async def test_ingest_title_falls_back_to_filename(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text="some text here for the test")],
        filename="interview.txt",
    )

    assert result.documents[0].title == "interview.txt"


# ---------------------------------------------------------------------------
# list_documents / list_chunks
# ---------------------------------------------------------------------------


async def test_list_documents_paginated(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    docs = [DocumentInput(title=f"Doc {i}", text=f"Document number {i}") for i in range(5)]
    await svc.ingest_documents(corpus_id=corpus.id, documents=docs)

    page1, total = await svc.list_documents(corpus_id=corpus.id, page=1, page_size=3)
    assert total == 5
    assert len(page1) == 3

    page2, _ = await svc.list_documents(corpus_id=corpus.id, page=2, page_size=3)
    assert len(page2) == 2


async def test_list_chunks_for_corpus(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    text = " ".join(str(i) for i in range(20))
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text=text)],
    )

    chunks, total = await svc.list_chunks(corpus_id=corpus.id)
    assert total == result.chunks_created


async def test_list_chunks_filter_by_document(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    text = " ".join(str(i) for i in range(20))
    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="A", text=text), DocumentInput(title="B", text="short text here now")],
    )

    doc_id = result.documents[0].id
    chunks, _ = await svc.list_chunks(corpus_id=corpus.id, document_id=doc_id)
    assert all(c.document_id == doc_id for c in chunks)
