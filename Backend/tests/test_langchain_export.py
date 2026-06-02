import uuid

from app.schemas.ingestion import CorpusCreate, DocumentInput
from app.services.ingestion import IngestionService
from app.services.langchain_export import load_corpus_documents_as_langchain_documents

P1 = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def test_load_returns_one_document_per_ingested_doc(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[
            DocumentInput(title="A", text="first document text"),
            DocumentInput(title="B", text="second document text"),
        ],
    )

    docs = await load_corpus_documents_as_langchain_documents(db_session, corpus.id)
    assert len(docs) == 2


async def test_load_document_metadata_keys(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text="some text content here")],
    )

    docs = await load_corpus_documents_as_langchain_documents(db_session, corpus.id)
    assert docs
    assert {"corpus_id", "document_id"}.issubset(docs[0].metadata.keys())


async def test_load_metadata_corpus_id_correct(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text="some text content here")],
    )

    docs = await load_corpus_documents_as_langchain_documents(db_session, corpus.id)
    for doc in docs:
        assert doc.metadata["corpus_id"] == str(corpus.id)


async def test_load_page_content_is_text(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text="the full document content here")],
    )

    docs = await load_corpus_documents_as_langchain_documents(db_session, corpus.id)
    for doc in docs:
        assert isinstance(doc.page_content, str)
        assert doc.page_content.strip()


async def test_load_empty_corpus_returns_empty_list(db_session):
    svc = IngestionService(db_session)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    docs = await load_corpus_documents_as_langchain_documents(db_session, corpus.id)
    assert docs == []
