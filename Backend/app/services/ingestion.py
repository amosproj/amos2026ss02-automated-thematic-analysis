import uuid
from dataclasses import dataclass, field

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.exceptions import NotFoundError, UnprocessableError
from app.models.ingestion import Corpus, CorpusChunk, CorpusDocument
from app.schemas.ingestion import CorpusCreate, DocumentInput
from app.services.text_chunking import chunk_text_by_words


@dataclass
class IngestResult:
    """Internal result of an ingestion call. Converted to IngestResultSchema before returning to the client."""

    documents: list[CorpusDocument] = field(default_factory=list)
    chunks_created: int = 0


# ---------------------------------------------------------------------------
# Ingestion service
# ---------------------------------------------------------------------------


class IngestionService:
    """Handles all database operations for corpora, documents, and chunks."""

    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self._session = session
        self._settings = settings

    async def create_corpus(self, payload: CorpusCreate) -> Corpus:
        """Insert a new corpus and return the refreshed ORM object."""
        corpus = Corpus(
            project_id=payload.project_id, # TODO: Only placeholder for now. add Project Data Structure and wire correctly into Corpus
            name=payload.name,
        )
        self._session.add(corpus)
        await self._session.commit()
        await self._session.refresh(corpus)
        return corpus

    async def get_corpus(self, corpus_id: uuid.UUID) -> Corpus:
        """Fetch a corpus by ID. Raises NotFoundError if it doesn't exist."""
        result = await self._session.execute(
            select(Corpus).where(Corpus.id == corpus_id)
        )
        corpus = result.scalar_one_or_none()
        if corpus is None:
            raise NotFoundError(f"Corpus '{corpus_id}' not found")
        return corpus

    async def list_corpora(
        self,
        project_id: uuid.UUID | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Corpus], int]:
        """Return a paginated list of corpora, optionally filtered by project_id."""
        base = select(Corpus)
        count_q = select(func.count()).select_from(Corpus)
        if project_id is not None:
            base = base.where(Corpus.project_id == project_id)
            count_q = count_q.where(Corpus.project_id == project_id)

        total: int = (await self._session.execute(count_q)).scalar_one()

        offset = (page - 1) * page_size
        rows = await self._session.execute(
            base.order_by(Corpus.created_at.desc()).offset(offset).limit(page_size)
        )
        return list(rows.scalars().all()), total

    async def _resolve_filename_conflict(
        self, corpus_id: uuid.UUID, filename: str
    ) -> str:
        """Lowercase the filename; if it collides within the corpus, append ' (n)'
        before the extension until unique. e.g. 'Interview.TXT' after
        'interview.txt' → 'interview (2).txt'."""
        filename = filename.lower()
        existing = set(
            (
                await self._session.execute(
                    select(CorpusDocument.filename).where(
                        CorpusDocument.corpus_id == corpus_id,
                        CorpusDocument.filename.is_not(None),
                    )
                )
            ).scalars()
        )
        if filename not in existing:
            return filename

        dot = filename.rfind(".")
        stem, ext = (filename[:dot], filename[dot:]) if dot != -1 else (filename, "")
        n = 2
        while f"{stem} ({n}){ext}" in existing:
            n += 1
        return f"{stem} ({n}){ext}"

    async def ingest_documents(
        self,
        corpus_id: uuid.UUID,
        documents: list[DocumentInput],
        filename: str | None = None,
    ) -> IngestResult:
        """For each non-empty document: insert a CorpusDocument, chunk its text,
        insert CorpusChunk rows. One commit at the end; rolls back on any failure.
        `filename` (if given) is deduplicated and stored on every created document."""
        await self.get_corpus(corpus_id)

        stored_filename: str | None = None
        if filename:
            stored_filename = await self._resolve_filename_conflict(corpus_id, filename)

        result = IngestResult()
        try:
            for doc_input in documents:
                text = (doc_input.text or "").strip()
                if not text:
                    continue

                doc = CorpusDocument(
                    corpus_id=corpus_id,
                    title=doc_input.title or stored_filename or "Untitled",
                    filename=stored_filename,
                )
                self._session.add(doc)
                # Flush to get doc.id before inserting chunks that reference it.
                await self._session.flush()

                spans = chunk_text_by_words(
                    text,
                    chunk_size_words=self._settings.INGESTION_CHUNK_SIZE_WORDS,
                    overlap_words=self._settings.INGESTION_CHUNK_OVERLAP_WORDS,
                )
                for span in spans:
                    self._session.add(
                        CorpusChunk(
                            document_id=doc.id,
                            chunk_index=span.chunk_index,
                            text=span.text,
                        )
                    )

                result.documents.append(doc)
                result.chunks_created += len(spans)

            await self._session.commit()
        except Exception as exc:
            await self._session.rollback()
            raise UnprocessableError(f"Ingestion failed: {exc}") from exc

        return result

    async def list_documents(
        self,
        corpus_id: uuid.UUID,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[CorpusDocument], int]:
        """Return a paginated list of documents for a corpus."""
        count_q = (
            select(func.count())
            .select_from(CorpusDocument)
            .where(CorpusDocument.corpus_id == corpus_id)
        )
        total: int = (await self._session.execute(count_q)).scalar_one()

        offset = (page - 1) * page_size
        rows = await self._session.execute(
            select(CorpusDocument)
            .where(CorpusDocument.corpus_id == corpus_id)
            .order_by(CorpusDocument.created_at.desc())
            .offset(offset)
            .limit(page_size)
        )
        return list(rows.scalars().all()), total

    async def list_chunks(
        self,
        corpus_id: uuid.UUID,
        document_id: uuid.UUID | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[CorpusChunk], int]:
        """Return a paginated list of chunks for a corpus, optionally filtered to one document."""
        base = (
            select(CorpusChunk)
            .join(CorpusDocument, CorpusChunk.document_id == CorpusDocument.id)
            .where(CorpusDocument.corpus_id == corpus_id)
        )
        count_q = (
            select(func.count())
            .select_from(CorpusChunk)
            .join(CorpusDocument, CorpusChunk.document_id == CorpusDocument.id)
            .where(CorpusDocument.corpus_id == corpus_id)
        )
        if document_id is not None:
            base = base.where(CorpusChunk.document_id == document_id)
            count_q = count_q.where(CorpusChunk.document_id == document_id)

        total: int = (await self._session.execute(count_q)).scalar_one()

        offset = (page - 1) * page_size
        rows = await self._session.execute(
            base.order_by(CorpusDocument.id, CorpusChunk.chunk_index)
            .offset(offset)
            .limit(page_size)
        )
        return list(rows.scalars().all()), total
