import uuid

from langchain_core.documents import Document
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ingestion import CorpusChunk, CorpusDocument


async def load_corpus_chunks_as_langchain_documents(
    session: AsyncSession,
    corpus_id: uuid.UUID,
) -> list[Document]:
    result = await session.execute(
        select(CorpusChunk, CorpusDocument)
        .join(CorpusDocument, CorpusChunk.document_id == CorpusDocument.id)
        .where(CorpusDocument.corpus_id == corpus_id)
        .order_by(CorpusDocument.id, CorpusChunk.chunk_index)
    )

    return [
        Document(
            page_content=chunk.text,
            metadata={
                "corpus_id": str(corpus_id),
                "document_id": str(doc.id),
                "chunk_id": str(chunk.id),
                "chunk_index": chunk.chunk_index,
            },
        )
        for chunk, doc in result.all()
    ]
