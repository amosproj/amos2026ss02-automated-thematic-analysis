import uuid

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError, UnprocessableError
from app.models.demographic import DemographicFiles, DemographicRow
from app.models.ingestion import CorpusDocument


async def auto_link_demographics(session: AsyncSession, corpus_id: uuid.UUID) -> None:
    """Link any unlinked corpus documents to matching demographic rows.

    Matching is case- and whitespace-insensitive on document title vs interviewee_id.
    Already-linked documents are not touched.
    """
    unlinked_docs = list(
        (
            await session.execute(
                select(CorpusDocument)
                .where(CorpusDocument.corpus_id == corpus_id)
                .where(CorpusDocument.demographic_row_id.is_(None))
            )
        ).scalars()
    )

    if not unlinked_docs:
        return

    demo_rows = list(
        (
            await session.execute(
                select(DemographicRow)
                .join(DemographicFiles)
                .where(DemographicFiles.corpus_id == corpus_id)
            )
        ).scalars()
    )

    lookup = {row.interviewee_id.strip().lower(): row.id for row in demo_rows}

    for doc in unlinked_docs:
        matched_id = lookup.get(doc.title.strip().lower())
        if matched_id:
            doc.demographic_row_id = matched_id

    await session.commit()


async def _get_document_in_corpus(
    session: AsyncSession,
    corpus_id: uuid.UUID,
    document_id: uuid.UUID,
) -> CorpusDocument:
    """Load one corpus document, raising NotFoundError if it is not part of the corpus."""
    doc = (
        await session.execute(
            select(CorpusDocument).where(
                CorpusDocument.id == document_id,
                CorpusDocument.corpus_id == corpus_id,
            )
        )
    ).scalar_one_or_none()
    if doc is None:
        raise NotFoundError(f"Document '{document_id}' not found in corpus '{corpus_id}'")
    return doc


async def set_document_link(
    session: AsyncSession,
    corpus_id: uuid.UUID,
    document_id: uuid.UUID,
    demographic_row_id: uuid.UUID | None,
) -> CorpusDocument:
    """Manually link (or unlink) one transcript to a demographic row.

    Passing ``demographic_row_id=None`` clears the link. Linking uses reassign
    semantics: a demographic row maps to at most one transcript, so if the target
    row is already linked to a different document that other link is cleared first.
    Validates that both the document and the row belong to ``corpus_id``.
    """
    doc = await _get_document_in_corpus(session, corpus_id, document_id)

    if demographic_row_id is None:
        doc.demographic_row_id = None
        await session.commit()
        await session.refresh(doc)
        return doc

    row = (
        await session.execute(
            select(DemographicRow).where(
                DemographicRow.id == demographic_row_id,
                DemographicRow.corpus_id == corpus_id,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise UnprocessableError(
            f"Demographic row '{demographic_row_id}' does not belong to corpus '{corpus_id}'"
        )

    # Reassign: detach the row from any other document it is currently linked to
    # so the row never maps to two transcripts at once.
    await session.execute(
        update(CorpusDocument)
        .where(
            CorpusDocument.corpus_id == corpus_id,
            CorpusDocument.demographic_row_id == demographic_row_id,
            CorpusDocument.id != document_id,
        )
        .values(demographic_row_id=None)
    )

    doc.demographic_row_id = demographic_row_id
    await session.commit()
    await session.refresh(doc)
    return doc
