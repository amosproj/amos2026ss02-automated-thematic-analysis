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

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import _get_engine
from app.models import CodeAssignment
from app.services.quote_matching import (
    QuoteSpanCandidate,
    select_deduplicated_quote_spans,
)


async def backfill_deduplicate_code_assignments(session: AsyncSession) -> int:
    rows = await session.scalars(
        select(CodeAssignment).order_by(CodeAssignment.created_at, CodeAssignment.id)
    )
    quoted_rows = [row for row in rows if row.quote and row.quote.strip()]
    candidates = [
        QuoteSpanCandidate(
            group_key=(row.document_coding_id, row.code_id),
            quote=row.quote,
            start_char=row.start_char,
            end_char=row.end_char,
            confidence=row.confidence,
            quote_match_status=row.quote_match_status,
        )
        for row in quoted_rows
    ]
    kept = set(select_deduplicated_quote_spans(candidates))
    removed = 0
    for index, row in enumerate(quoted_rows):
        if index not in kept:
            await session.delete(row)
            removed += 1

    if removed:
        await session.commit()
    return removed


async def _main() -> None:
    engine = _get_engine()
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        removed = await backfill_deduplicate_code_assignments(session)
    print(f"Removed {removed} duplicate/overlapping code-assignment quote row(s).")


if __name__ == "__main__":
    asyncio.run(_main())
