import uuid

from langchain_core.documents import Document
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ingestion import CorpusDocument


async def load_corpus_documents_as_langchain_documents(
    session: AsyncSession,
    corpus_id: uuid.UUID,
) -> list[Document]:
    """Load all documents for a corpus as LangChain Documents for downstream consumption.

    Each Document carries the full document content as page_content and a metadata dict
    with corpus_id and document_id (both stringified for JSON serializability).
    Results are ordered by document creation time.
    """
    result = await session.execute(
        select(CorpusDocument)
        .where(CorpusDocument.corpus_id == corpus_id)
        .order_by(CorpusDocument.created_at)
    )

    return [
        Document(
            page_content=doc.content,
            metadata={
                "corpus_id": str(corpus_id),
                "document_id": str(doc.id),
            },
        )
        for doc in result.scalars().all()
        if doc.content and doc.content.strip()
    ]
