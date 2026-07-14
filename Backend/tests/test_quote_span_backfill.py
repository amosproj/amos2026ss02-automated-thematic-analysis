from __future__ import annotations

import uuid
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Code,
    CodeAssignment,
    Codebook,
    CodebookApplicationRun,
    Corpus,
    CorpusDocument,
    DocumentCoding,
    Theme,
)
from app.services.quote_span_backfill import backfill_deduplicate_code_assignments

_TRANSCRIPT = "Participant: The manual handoffs slow everyone down, honestly."


async def _seed(session: AsyncSession) -> tuple[UUID, UUID, UUID, UUID]:
    """Return (coding_1_id, coding_2_id, code_a_id, code_b_id)."""
    corpus = Corpus(id=uuid.uuid4(), project_id=uuid.uuid4(), name="Corpus")
    codebook = Codebook(
        id=uuid.uuid4(),
        corpus_id=corpus.id,
        name="Backfill codebook",
        description="Fixture",
        version=1,
        created_by="system",
    )
    # A single shared theme: two codes under it must stay independent.
    theme = Theme(id=uuid.uuid4(), codebook_id=codebook.id, label="Workflow Friction", is_active=True)
    code_a = Code(id=uuid.uuid4(), codebook_id=codebook.id, label="Manual Handoffs", is_active=True)
    code_b = Code(id=uuid.uuid4(), codebook_id=codebook.id, label="Handoff Delays", is_active=True)
    run = CodebookApplicationRun(
        id=uuid.uuid4(), corpus_id=corpus.id, codebook_id=codebook.id, status="succeeded"
    )
    documents = [
        CorpusDocument(id=uuid.uuid4(), corpus_id=corpus.id, title="Doc 1", content=_TRANSCRIPT),
        CorpusDocument(id=uuid.uuid4(), corpus_id=corpus.id, title="Doc 2", content=_TRANSCRIPT),
    ]
    codings = [
        DocumentCoding(
            id=uuid.uuid4(),
            application_run_id=run.id,
            document_id=documents[index].id,
            codebook_id=codebook.id,
        )
        for index in range(2)
    ]
    session.add_all([corpus, codebook, theme, code_a, code_b, run, *documents, *codings])
    await session.flush()

    def _assignment(
        coding_id: UUID,
        code_id: UUID,
        quote: str,
        start: int | None,
        end: int | None,
    ) -> CodeAssignment:
        return CodeAssignment(
            id=uuid.uuid4(),
            document_coding_id=coding_id,
            code_id=code_id,
            theme_id=theme.id,
            quote=quote,
            start_char=start,
            end_char=end,
            quote_match_status="exact",
            confidence=0.9,
        )

    c1, c2 = codings[0].id, codings[1].id
    session.add_all([
        # code_a, same passage twice plus a longer overlapping span -> collapse
        # to the single longest span.
        _assignment(c1, code_a.id, "manual handoffs slow", 17, 37),
        _assignment(c1, code_a.id, "manual handoffs slow", 17, 37),
        _assignment(c1, code_a.id, "The manual handoffs slow everyone down", 13, 51),
        # code_a, a distinct passage -> kept.
        _assignment(c1, code_a.id, "honestly", 53, 61),
        # code_b, the SAME passage under a different code of the SAME theme ->
        # kept: distinct codes never dedup against each other.
        _assignment(c1, code_b.id, "manual handoffs slow", 17, 37),
        # Empty/whitespace quote -> ignored by the backfill, left untouched.
        _assignment(c1, code_a.id, "   ", None, None),
        # code_a, same passage in a different document coding -> kept.
        _assignment(c2, code_a.id, "manual handoffs slow", 17, 37),
    ])
    await session.commit()
    return c1, c2, code_a.id, code_b.id


async def _quotes_for(session: AsyncSession, coding_id: UUID, code_id: UUID) -> list[str]:
    rows = (
        await session.scalars(
            select(CodeAssignment.quote).where(
                CodeAssignment.document_coding_id == coding_id,
                CodeAssignment.code_id == code_id,
            )
        )
    ).all()
    return sorted(rows)


async def test_backfill_collapses_same_code_overlaps_but_keeps_distinct_codes(db_session) -> None:
    c1, c2, code_a_id, code_b_id = await _seed(db_session)

    removed = await backfill_deduplicate_code_assignments(db_session)

    # Only the two duplicate code_a spans of the passage are deleted.
    assert removed == 2

    # code_a keeps its longest span, the distinct quote, and the untouched
    # whitespace row.
    assert await _quotes_for(db_session, c1, code_a_id) == [
        "   ",
        "The manual handoffs slow everyone down",
        "honestly",
    ]

    # code_b tagged the same passage under the same theme -> untouched.
    assert await _quotes_for(db_session, c1, code_b_id) == ["manual handoffs slow"]

    # The identical passage in a different document coding is untouched.
    assert await _quotes_for(db_session, c2, code_a_id) == ["manual handoffs slow"]


async def test_backfill_is_idempotent(db_session) -> None:
    await _seed(db_session)

    first = await backfill_deduplicate_code_assignments(db_session)
    second = await backfill_deduplicate_code_assignments(db_session)

    assert first == 2
    assert second == 0
