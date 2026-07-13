"""
One-time cleanup of duplicate/overlapping code-assignment quotes of the old runs.

It is idempotent: once a coding is clean, re-running deletes nothing.

Run it from a source checkout with the DB reachable via:

    python -m app.services.quote_span_backfill

This is a self-contained, throwaway migration. Once every pre-fix run is
cleaned, delete this module and tests/test_quote_span_backfill.py
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import _get_engine
from app.models import CodeAssignment, CorpusDocument, DocumentCoding
from app.services.quote_matching import QuoteSpanCandidate, merge_quote_spans


async def backfill_deduplicate_code_assignments(session: AsyncSession) -> int:
    rows = list(
        (
            await session.scalars(
                select(CodeAssignment).order_by(
                    CodeAssignment.created_at, CodeAssignment.id
                )
            )
        ).all()
    )

    rows_by_coding: dict[UUID, list[CodeAssignment]] = defaultdict(list)
    for row in rows:
        if row.quote and row.quote.strip():
            rows_by_coding[row.document_coding_id].append(row)
    if not rows_by_coding:
        return 0

    transcript_by_coding = await _load_transcripts_by_coding(session, set(rows_by_coding))

    removed = 0
    for coding_id, coding_rows in rows_by_coding.items():
        transcript = transcript_by_coding.get(coding_id, "")
        if not transcript:
            # Without the document text we cannot re-slice a merged span, so
            # leave these rows untouched rather than risk blanking a quote.
            continue
        candidates = [
            QuoteSpanCandidate(
                # Group strictly per code: distinct codes on the same passage
                # are both kept, so no code loses coverage to a sibling.
                group_key=row.code_id,
                quote=row.quote,
                start_char=row.start_char,
                end_char=row.end_char,
                confidence=row.confidence,
            )
            for row in coding_rows
        ]
        kept: set[int] = set()
        for merged in merge_quote_spans(candidates, transcript=transcript):
            kept.add(merged.primary_index)
            if merged.merged:
                primary = coding_rows[merged.primary_index]
                primary.quote = merged.quote
                primary.start_char = merged.start_char
                primary.end_char = merged.end_char
                primary.quote_match_status = "exact"
        for index, row in enumerate(coding_rows):
            if index not in kept:
                await session.delete(row)
                removed += 1

    await session.commit()
    return removed


async def _load_transcripts_by_coding(
    session: AsyncSession, coding_ids: set[UUID]
) -> dict[UUID, str]:
    document_id_by_coding: dict[UUID, UUID] = {
        coding_id: document_id
        for coding_id, document_id in (
            await session.execute(
                select(DocumentCoding.id, DocumentCoding.document_id).where(
                    DocumentCoding.id.in_(coding_ids)
                )
            )
        ).all()
    }
    content_by_document: dict[UUID, str | None] = {
        document_id: content
        for document_id, content in (
            await session.execute(
                select(CorpusDocument.id, CorpusDocument.content).where(
                    CorpusDocument.id.in_(set(document_id_by_coding.values()))
                )
            )
        ).all()
    }
    return {
        coding_id: content_by_document.get(document_id) or ""
        for coding_id, document_id in document_id_by_coding.items()
    }


async def _main() -> None:
    engine = _get_engine()
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        removed = await backfill_deduplicate_code_assignments(session)
    print(f"Removed {removed} duplicate/overlapping code-assignment quote row(s).")


if __name__ == "__main__":
    asyncio.run(_main())
