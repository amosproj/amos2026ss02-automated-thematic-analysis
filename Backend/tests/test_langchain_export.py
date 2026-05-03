import uuid

from app.schemas.ingestion import CorpusCreate, DocumentInput
from app.services.ingestion import IngestionService
from app.services.langchain_export import load_corpus_chunks_as_langchain_documents

P1 = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def test_load_returns_langchain_documents(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    # 20 words, chunk_size=10, overlap=2 → stride=8 → 3 chunks
    text = " ".join(str(i) for i in range(20))
    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text=text)],
    )

    docs = await load_corpus_chunks_as_langchain_documents(db_session, corpus.id)
    assert len(docs) == 3


async def test_load_document_metadata_keys(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text=" ".join(str(i) for i in range(12)))],
    )

    docs = await load_corpus_chunks_as_langchain_documents(db_session, corpus.id)
    assert docs

    assert {"corpus_id", "document_id", "chunk_id", "chunk_index"}.issubset(docs[0].metadata.keys())


async def test_load_metadata_corpus_id_correct(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text=" ".join(str(i) for i in range(12)))],
    )

    docs = await load_corpus_chunks_as_langchain_documents(db_session, corpus.id)
    for doc in docs:
        assert doc.metadata["corpus_id"] == str(corpus.id)


async def test_load_chunk_index_sequential(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text=" ".join(str(i) for i in range(20)))],
    )

    docs = await load_corpus_chunks_as_langchain_documents(db_session, corpus.id)
    assert [d.metadata["chunk_index"] for d in docs] == list(range(len(docs)))


async def test_load_page_content_is_text(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    await svc.ingest_documents(
        corpus_id=corpus.id,
        documents=[DocumentInput(title="T", text=" ".join(str(i) for i in range(12)))],
    )

    docs = await load_corpus_chunks_as_langchain_documents(db_session, corpus.id)
    for doc in docs:
        assert isinstance(doc.page_content, str)
        assert doc.page_content.strip()


async def test_load_empty_corpus_returns_empty_list(db_session, test_settings):
    svc = IngestionService(db_session, test_settings)
    corpus = await svc.create_corpus(CorpusCreate(project_id=P1, name="C"))

    docs = await load_corpus_chunks_as_langchain_documents(db_session, corpus.id)
    assert docs == []
