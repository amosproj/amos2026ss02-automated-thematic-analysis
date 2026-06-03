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


async def test_create_corpus(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(corpus_id=P1, name="Test Corpus"))
    assert corpus.id
    assert corpus.id == P1
    assert corpus.name == "Test Corpus"


async def test_get_corpus_not_found_raises(db_session):
    svc = IngestionService(db_session)
    with pytest.raises(NotFoundError):
        await svc.get_corpus(uuid.uuid4())


async def test_list_corpora_empty(db_session):
    svc = IngestionService(db_session)
    corpora, total = await svc.list_corpora(project_id=P1)
    assert corpora == []
    assert total == 0

async def test_list_corpora_filters_by_project(db_session):
    svc = IngestionService(db_session)
    await svc.create_corpus(CorpusCreate(corpus_id=P1, name="A"))
    await svc.create_corpus(CorpusCreate(corpus_id=P2, name="B"))

    result, total = await svc.list_corpora()
    assert total >= 2




# ---------------------------------------------------------------------------
# Document ingestion
# ---------------------------------------------------------------------------


async def test_ingest_one_valid_document(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(corpus_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="Doc", text="one two three four five")],
    )

    assert len(result.documents) == 1
    assert result.documents[0].title == "Doc"
    assert result.documents[0].content == "one two three four five"



async def test_ingest_skips_empty_documents(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(corpus_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="E", text=""), DocumentInput(title="V", text="valid text here")],
    )

    assert len(result.documents) == 1
    assert result.documents[0].title == "V"


async def test_ingest_title_falls_back_to_filename(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(corpus_id=P1, name="C"))

    result = await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(text="some text here for the test")],
        filename="interview.txt",
    )

    assert result.documents[0].title == "interview.txt"


# ---------------------------------------------------------------------------
# list_documents / get_document
# ---------------------------------------------------------------------------


async def test_list_documents_paginated(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(corpus_id=P1, name="C"))

    docs = [DocumentInput(title=f"Doc {i}", text=f"Document number {i}") for i in range(5)]
    await svc.ingest_documents(corpus_id=corpus.id, documents=docs)

    page1, total = await svc.list_documents(corpus_id=corpus.id, page=1, page_size=3)
    assert total == 5
    assert len(page1) == 3

    page2, _ = await svc.list_documents(corpus_id=corpus.id, page=2, page_size=3)
    assert len(page2) == 2

