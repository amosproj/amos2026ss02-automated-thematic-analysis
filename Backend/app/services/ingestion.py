import csv
import io
import json
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
    documents: list[CorpusDocument] = field(default_factory=list)
    chunks_created: int = 0


# ---------------------------------------------------------------------------
# File parsers
# ---------------------------------------------------------------------------


def parse_text_upload(filename: str, content: bytes) -> list[DocumentInput]:
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UnprocessableError(f"Could not decode '{filename}' as UTF-8") from exc
    return [DocumentInput(title=filename, text=text)]


def parse_json_upload(filename: str, content: bytes) -> list[DocumentInput]:
    try:
        data = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UnprocessableError(f"Invalid JSON in '{filename}': {exc}") from exc

    if isinstance(data, list):
        items = data
    elif isinstance(data, dict) and "documents" in data:
        items = data["documents"]
    else:
        raise UnprocessableError(
            f"'{filename}': JSON must be a list of documents or an object with a 'documents' key"
        )

    docs: list[DocumentInput] = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise UnprocessableError(f"'{filename}': document at index {i} must be an object")
        if "text" not in item:
            raise UnprocessableError(f"'{filename}': document at index {i} is missing 'text'")
        docs.append(
            DocumentInput(
                title=item.get("title") or filename,
                text=item["text"],
            )
        )
    return docs


def parse_csv_upload(filename: str, content: bytes) -> list[DocumentInput]:
    try:
        text_content = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UnprocessableError(f"Could not decode '{filename}' as UTF-8") from exc

    reader = csv.DictReader(io.StringIO(text_content))
    if not reader.fieldnames or "text" not in reader.fieldnames:
        raise UnprocessableError(f"'{filename}': CSV must contain a 'text' column")

    docs: list[DocumentInput] = []
    for i, row in enumerate(reader):
        text = row.get("text", "").strip()
        if not text:
            continue
        docs.append(
            DocumentInput(
                title=row.get("title") or f"{filename}:{i + 1}",
                text=text,
            )
        )
    return docs


def parse_jsonl_upload(filename: str, content: bytes) -> list[DocumentInput]:
    try:
        text_content = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UnprocessableError(f"Could not decode '{filename}' as UTF-8") from exc

    participants: dict[str, list[dict]] = {}
    for line_no, raw in enumerate(text_content.splitlines(), start=1):
        raw = raw.strip()
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise UnprocessableError(f"'{filename}': invalid JSON on line {line_no}: {exc}") from exc

        username = record.get("username")
        if not username:
            raise UnprocessableError(f"'{filename}': line {line_no} is missing 'username'")

        participants.setdefault(username, []).append(record)

    if not participants:
        raise UnprocessableError(f"'{filename}': file contains no records")

    docs: list[DocumentInput] = []
    for username, messages in participants.items():
        messages.sort(key=lambda m: m.get("message_index", 0))

        human_turns = [
            m for m in messages
            if m.get("event_type") == "human_response"
            and str(m.get("message_content", "")).strip()
        ]
        if not human_turns:
            continue

        text = "\n\n".join(str(m["message_content"]) for m in human_turns)
        docs.append(DocumentInput(title=username, text=text))
    return docs


# ---------------------------------------------------------------------------
# Ingestion service
# ---------------------------------------------------------------------------


class IngestionService:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self._session = session
        self._settings = settings

    async def create_corpus(self, payload: CorpusCreate) -> Corpus:
        corpus = Corpus(
            project_id=payload.project_id,
            name=payload.name,
        )
        self._session.add(corpus)
        await self._session.commit()
        await self._session.refresh(corpus)
        return corpus

    async def get_corpus(self, corpus_id: uuid.UUID) -> Corpus:
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

    async def ingest_documents(
        self,
        corpus_id: uuid.UUID,
        documents: list[DocumentInput],
        filename: str | None = None,
    ) -> IngestResult:
        await self.get_corpus(corpus_id)

        result = IngestResult()
        try:
            for doc_input in documents:
                text = (doc_input.text or "").strip()
                if not text:
                    continue

                doc = CorpusDocument(
                    corpus_id=corpus_id,
                    title=doc_input.title or filename or "Untitled",
                )
                self._session.add(doc)
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
