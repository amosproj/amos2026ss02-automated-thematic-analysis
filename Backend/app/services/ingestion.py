import csv
import io
import json
import uuid
from dataclasses import dataclass, field

from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.domain.enums import DocumentStatus, IngestionRunStatus, SourceType
from app.exceptions import NotFoundError, UnprocessableError
from app.models.ingestion import Corpus, CorpusChunk, CorpusDocument, IngestionRun
from app.schemas.ingestion import CorpusCreate, DocumentInput
from app.services.text_chunking import chunk_text_by_words, count_words, sha256_text


@dataclass
class IngestionResult:
    run: IngestionRun
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
    return [DocumentInput(external_id=filename, title=filename, text=text)]


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
                external_id=item.get("external_id") or None,
                title=item.get("title") or None,
                text=item["text"],
                metadata=item.get("metadata") or {},
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

    known_columns = {"text", "external_id", "title"}
    docs: list[DocumentInput] = []
    for row in reader:
        text = row.get("text", "").strip()
        if not text:
            continue
        extra = {k: v for k, v in row.items() if k not in known_columns and v is not None}
        docs.append(
            DocumentInput(
                external_id=row.get("external_id") or None,
                title=row.get("title") or None,
                text=text,
                metadata=extra,
            )
        )
    return docs


def parse_jsonl_upload(filename: str, content: bytes) -> list[DocumentInput]:
    try:
        text_content = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UnprocessableError(f"Could not decode '{filename}' as UTF-8") from exc

    # Collect messages per participant, preserving order
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
        docs.append(
            DocumentInput(
                external_id=username,
                title=username,
                text=text,
                metadata={
                    "total_turns": len(messages),
                    "human_turns": len(human_turns),
                },
            )
        )
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
            id=uuid.uuid4(),
            project_id=payload.project_id,
            name=payload.name,
            description=payload.description,
            research_question=payload.research_question,
            extra_metadata=payload.metadata,
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
        source_type: SourceType,
        filename: str | None = None,
    ) -> IngestionResult:
        await self.get_corpus(corpus_id)  # raises NotFoundError early if missing

        run_id = uuid.uuid4()
        run = IngestionRun(
            id=run_id,
            corpus_id=corpus_id,
            source_type=source_type,
            status=IngestionRunStatus.RUNNING,
            filename=filename,
            total_documents=0,
            accepted_documents=0,
            rejected_documents=0,
            duplicate_documents=0,
            empty_documents=0,
            parameters={
                "chunk_size_words": self._settings.INGESTION_CHUNK_SIZE_WORDS,
                "chunk_overlap_words": self._settings.INGESTION_CHUNK_OVERLAP_WORDS,
                "deduplicate_by_hash": self._settings.INGESTION_DEDUPLICATE_BY_HASH,
            },
        )
        self._session.add(run)

        accepted_docs: list[CorpusDocument] = []
        chunks_created = 0

        try:
            await self._session.flush()

            for doc_input in documents:
                text = (doc_input.text or "").strip()
                word_count = count_words(text)

                if not text or word_count == 0:
                    run.empty_documents += 1
                    run.rejected_documents += 1
                    continue

                if word_count > self._settings.INGESTION_MAX_DOCUMENT_WORDS:
                    run.rejected_documents += 1
                    logger.debug(
                        "Document rejected: {} words > max {}",
                        word_count,
                        self._settings.INGESTION_MAX_DOCUMENT_WORDS,
                    )
                    continue

                text_hash = sha256_text(text)

                if self._settings.INGESTION_DEDUPLICATE_BY_HASH:
                    dup = await self._session.execute(
                        select(CorpusDocument)
                        .where(CorpusDocument.corpus_id == corpus_id)
                        .where(CorpusDocument.text_hash == text_hash)
                        .where(CorpusDocument.status == DocumentStatus.ACTIVE)
                        .limit(1)
                    )
                    if dup.scalar_one_or_none() is not None:
                        run.duplicate_documents += 1
                        continue

                doc = CorpusDocument(
                    id=uuid.uuid4(),
                    corpus_id=corpus_id,
                    external_id=doc_input.external_id,
                    title=doc_input.title,
                    text=text,
                    text_hash=text_hash,
                    word_count=word_count,
                    source_type=source_type,
                    status=DocumentStatus.ACTIVE,
                    extra_metadata=doc_input.metadata,
                )
                self._session.add(doc)
                await self._session.flush()  # guarantees FK is satisfied before chunks

                spans = chunk_text_by_words(
                    text,
                    chunk_size_words=self._settings.INGESTION_CHUNK_SIZE_WORDS,
                    overlap_words=self._settings.INGESTION_CHUNK_OVERLAP_WORDS,
                )
                for span in spans:
                    self._session.add(
                        CorpusChunk(
                            id=uuid.uuid4(),
                            document_id=doc.id,
                            chunk_index=span.chunk_index,
                            start_word=span.start_word,
                            end_word=span.end_word,
                            text=span.text,
                            text_hash=sha256_text(span.text),
                            word_count=span.end_word - span.start_word,
                        )
                    )

                accepted_docs.append(doc)
                chunks_created += len(spans)
                run.accepted_documents += 1

            run.total_documents = (
                run.accepted_documents + run.rejected_documents + run.duplicate_documents
            )
            run.status = IngestionRunStatus.COMPLETED
            await self._session.commit()

        except Exception as exc:
            await self._session.rollback()
            logger.exception("Ingestion failed for corpus {}", corpus_id)
            # Best-effort: persist a FAILED run so the caller can inspect it
            try:
                self._session.add(
                    IngestionRun(
                        id=run_id,
                        corpus_id=corpus_id,
                        source_type=source_type,
                        status=IngestionRunStatus.FAILED,
                        filename=filename,
                        total_documents=0,
                        accepted_documents=0,
                        rejected_documents=0,
                        duplicate_documents=0,
                        empty_documents=0,
                        parameters={},
                        error_message=str(exc)[:2000],
                    )
                )
                await self._session.commit()
            except Exception:
                logger.exception("Could not persist failed ingestion run {}", run_id)
            raise UnprocessableError(f"Ingestion failed: {exc}") from exc

        return IngestionResult(run=run, documents=accepted_docs, chunks_created=chunks_created)

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

    async def get_ingestion_run(self, run_id: uuid.UUID) -> IngestionRun:
        result = await self._session.execute(
            select(IngestionRun).where(IngestionRun.id == run_id)
        )
        run = result.scalar_one_or_none()
        if run is None:
            raise NotFoundError(f"Ingestion run '{run_id}' not found")
        return run
