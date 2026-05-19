import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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
