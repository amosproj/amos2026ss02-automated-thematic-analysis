import uuid
from dataclasses import dataclass, field

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.exceptions import NotFoundError, UnprocessableError
from app.models.demographic import DemographicFiles, DemographicRow
from app.models.ingestion import Corpus, CorpusDocument
from app.schemas.ingestion import CorpusCreate, DocumentInput
from app.services.analysis_dependency_guard import (
    guard_corpus_deletion,
    guard_document_deletion,
)
from app.services.linking import auto_link_demographics


@dataclass
class IngestResult:
    """Internal result of an ingestion call. Converted to IngestResultSchema before returning to the client."""

    documents: list[CorpusDocument] = field(default_factory=list)
    missing_document_ids: list[uuid.UUID] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Ingestion service
# ---------------------------------------------------------------------------


class IngestionService:
    """Handles all database operations for corpora and documents."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_corpus(self, payload: CorpusCreate) -> Corpus:
        """Insert a new corpus and return the refreshed ORM object."""
        corpus = Corpus(
            id=payload.corpus_id,
            project_id=uuid.uuid4(), # TODO: Only placeholder for now. add Project Data Structure and wire correctly into Corpus
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
        corpus_id: uuid.UUID | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[Corpus], int]:
        """Return a paginated list of corpora, optionally filtered by corpus_id."""
        base = select(Corpus)
        count_q = select(func.count()).select_from(Corpus)
        if corpus_id is not None:
            base = base.where(Corpus.id == corpus_id)
            count_q = count_q.where(Corpus.id == corpus_id)

        total: int = (await self._session.execute(count_q)).scalar_one()

        offset = (page - 1) * page_size
        rows = await self._session.execute(
            base.order_by(Corpus.created_at.desc()).offset(offset).limit(page_size)
        )
        return list(rows.scalars().all()), total

    async def delete_corpus(self, corpus_id: uuid.UUID, *, force: bool = False) -> None:
        """Delete a corpus and all its associated data.
        Relies on database CASCADE ON DELETE constraints.
        """
        corpus = await self.get_corpus(corpus_id)
        await guard_corpus_deletion(
            self._session,
            corpus_id=corpus_id,
            force=force,
        )
        await self._session.delete(corpus)
        await self._session.commit()

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
        """For each non-empty document: insert a CorpusDocument with its full text content.
        One commit at the end; rolls back on any failure.
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
                    content=text,
                )
                self._session.add(doc)
                result.documents.append(doc)

            await self._session.commit()
        except Exception as exc:
            await self._session.rollback()
            raise UnprocessableError(f"Ingestion failed: {exc}") from exc

        await auto_link_demographics(self._session, corpus_id)

        return result

    async def copy_documents(
        self,
        source_corpus_id: uuid.UUID,
        target_corpus_id: uuid.UUID,
        document_ids: list[uuid.UUID]
    ) -> IngestResult:
        """Copy multiple documents to a target corpus.

        Also copies the demographic files and rows from the source corpus
        so that demographic data is preserved. Copied documents are linked
        to the newly created demographic rows.
        """
        await self.get_corpus(source_corpus_id)
        await self.get_corpus(target_corpus_id)

        docs = (await self._session.execute(
            select(CorpusDocument).where(
                CorpusDocument.corpus_id == source_corpus_id,
                CorpusDocument.id.in_(document_ids)
            )
        )).scalars().all()

        found_ids = {doc.id for doc in docs}
        missing = [did for did in document_ids if did not in found_ids]

        if not docs:
            return IngestResult(missing_document_ids=missing)

        # --- Copy demographic files and rows from the source corpus ----------
        old_row_to_new: dict[uuid.UUID, uuid.UUID] = {}
        needed_row_ids = {doc.demographic_row_id for doc in docs if doc.demographic_row_id}

        result = IngestResult(missing_document_ids=missing)
        try:
            if needed_row_ids:
                source_demo_files = (await self._session.execute(
                    select(DemographicFiles)
                    .where(DemographicFiles.corpus_id == source_corpus_id)
                )).scalars().all()

                # Pre-fetch existing demographic files in target corpus
                target_demo_files_raw = (await self._session.execute(
                    select(DemographicFiles)
                    .where(DemographicFiles.corpus_id == target_corpus_id)
                )).scalars().all()
                target_demo_files = {df.name: df for df in target_demo_files_raw}

                # Pre-fetch existing demographic rows in target corpus
                target_rows_raw = (await self._session.execute(
                    select(DemographicRow)
                    .where(DemographicRow.corpus_id == target_corpus_id)
                )).scalars().all()
                target_rows = {r.interviewee_id: r for r in target_rows_raw}

                for demo_file in source_demo_files:
                    source_rows = (await self._session.execute(
                        select(DemographicRow)
                        .where(
                            DemographicRow.demographic_file_id == demo_file.id,
                            DemographicRow.id.in_(needed_row_ids)
                        )
                    )).scalars().all()

                    if not source_rows:
                        continue

                    new_demo_file = target_demo_files.get(demo_file.name)
                    if not new_demo_file:
                        new_demo_file = DemographicFiles(
                            name=demo_file.name,
                            original_columns=demo_file.original_columns,
                            corpus_id=target_corpus_id,
                        )
                        self._session.add(new_demo_file)
                        await self._session.flush()
                        target_demo_files[new_demo_file.name] = new_demo_file

                    for row in source_rows:
                        new_row = target_rows.get(row.interviewee_id)
                        if not new_row:
                            new_row = DemographicRow(
                                demographic_file_id=new_demo_file.id,
                                corpus_id=target_corpus_id,
                                row_number=row.row_number,
                                interviewee_id=row.interviewee_id,
                                data=row.data,
                            )
                            self._session.add(new_row)
                            await self._session.flush()
                            target_rows[new_row.interviewee_id] = new_row
                        old_row_to_new[row.id] = new_row.id

            # --- Copy documents and re-link demographics ---------------------
            for doc in docs:
                new_row_id = old_row_to_new.get(doc.demographic_row_id) if doc.demographic_row_id else None
                new_doc = CorpusDocument(
                    corpus_id=target_corpus_id,
                    title=doc.title,
                    filename=doc.filename,
                    content=doc.content,
                    demographic_row_id=new_row_id,
                )
                self._session.add(new_doc)
                result.documents.append(new_doc)
            await self._session.commit()
        except Exception as exc:
            await self._session.rollback()
            raise UnprocessableError(f"Copy failed: {exc}") from exc

        return result

    async def create_corpus_with_documents(
        self,
        source_corpus_id: uuid.UUID,
        name: str,
        document_ids: list[uuid.UUID],
    ) -> tuple[Corpus, IngestResult]:
        """Atomically create a new corpus and copy documents into it.

        Also copies the demographic files and rows from the source corpus
        so that demographic data is preserved. Copied documents are linked
        to the newly created demographic rows.

        Everything happens in a single transaction: if any part fails
        (demographic copy, document copy), the corpus creation is rolled
        back as well — no orphaned empty corpora.
        """
        await self.get_corpus(source_corpus_id)

        docs = (await self._session.execute(
            select(CorpusDocument).where(
                CorpusDocument.corpus_id == source_corpus_id,
                CorpusDocument.id.in_(document_ids),
            )
        )).scalars().all()

        found_ids = {doc.id for doc in docs}
        missing = [did for did in document_ids if did not in found_ids]

        if not docs:
            raise NotFoundError(
                f"None of the requested documents were found in corpus '{source_corpus_id}'"
            )

        try:
            # --- Create the new corpus ---------------------------------------
            corpus = Corpus(
                id=uuid.uuid4(),
                project_id=uuid.uuid4(),
                name=name,
            )
            self._session.add(corpus)
            await self._session.flush()  # assign corpus.id

            # --- Copy demographic files and rows -----------------------------
            old_row_to_new: dict[uuid.UUID, uuid.UUID] = {}
            needed_row_ids = {doc.demographic_row_id for doc in docs if doc.demographic_row_id}

            if needed_row_ids:
                source_demo_files = (await self._session.execute(
                    select(DemographicFiles)
                    .where(DemographicFiles.corpus_id == source_corpus_id)
                )).scalars().all()

                for demo_file in source_demo_files:
                    source_rows = (await self._session.execute(
                        select(DemographicRow)
                        .where(
                            DemographicRow.demographic_file_id == demo_file.id,
                            DemographicRow.id.in_(needed_row_ids)
                        )
                    )).scalars().all()

                    if not source_rows:
                        continue

                    new_demo_file = DemographicFiles(
                        name=demo_file.name,
                        original_columns=demo_file.original_columns,
                        corpus_id=corpus.id,
                    )
                    self._session.add(new_demo_file)
                    await self._session.flush()  # assign new_demo_file.id

                    for row in source_rows:
                        new_row = DemographicRow(
                            demographic_file_id=new_demo_file.id,
                            corpus_id=corpus.id,
                            row_number=row.row_number,
                            interviewee_id=row.interviewee_id,
                            data=row.data,
                        )
                        self._session.add(new_row)
                        await self._session.flush()  # assign new_row.id
                        old_row_to_new[row.id] = new_row.id

            # --- Copy documents and re-link demographics ---------------------
            result = IngestResult(missing_document_ids=missing)
            for doc in docs:
                new_row_id = old_row_to_new.get(doc.demographic_row_id) if doc.demographic_row_id else None
                new_doc = CorpusDocument(
                    corpus_id=corpus.id,
                    title=doc.title,
                    filename=doc.filename,
                    content=doc.content,
                    demographic_row_id=new_row_id,
                )
                self._session.add(new_doc)
                result.documents.append(new_doc)

            await self._session.commit()
            await self._session.refresh(corpus)
        except Exception as exc:
            await self._session.rollback()
            raise UnprocessableError(
                f"Failed to create corpus with documents: {exc}"
            ) from exc

        return corpus, result

    async def get_document(self, corpus_id: uuid.UUID, document_id: uuid.UUID) -> CorpusDocument:
        """Fetch a single document by ID within the given corpus. Raises NotFoundError if absent."""
        result = await self._session.execute(
            select(CorpusDocument)
            .options(joinedload(CorpusDocument.demographic_row))
            .where(
                CorpusDocument.id == document_id,
                CorpusDocument.corpus_id == corpus_id,
            )
        )
        doc = result.scalar_one_or_none()
        if doc is None:
            raise NotFoundError(f"Document '{document_id}' not found in corpus '{corpus_id}'")
        return doc

    async def delete_document(
        self,
        corpus_id: uuid.UUID,
        document_id: uuid.UUID,
        *,
        force: bool = False,
    ) -> None:
        """Delete a single document by ID. Raises NotFoundError if absent."""
        doc = await self.get_document(corpus_id, document_id)
        await guard_document_deletion(
            self._session,
            corpus_id=corpus_id,
            document_ids=[document_id],
            force=force,
        )
        await self._session.delete(doc)
        await self._session.commit()

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
