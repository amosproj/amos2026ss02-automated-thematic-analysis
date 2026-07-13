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
    """Return (coding_1_id, coding_2_id, theme_id, other_theme_id)."""
    corpus = Corpus(id=uuid.uuid4(), project_id=uuid.uuid4(), name="Corpus")
    codebook = Codebook(
        id=uuid.uuid4(),
        corpus_id=corpus.id,
        name="Backfill codebook",
        description="Fixture",
        version=1,
        created_by="system",
    )
    theme = Theme(id=uuid.uuid4(), codebook_id=codebook.id, label="Workflow Friction", is_active=True)
    other_theme = Theme(id=uuid.uuid4(), codebook_id=codebook.id, label="Other", is_active=True)
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
    session.add_all(
        [corpus, codebook, theme, other_theme, code_a, code_b, run, *documents, *codings]
    )
    await session.flush()

    def _assignment(
        coding_id: UUID,
        code_id: UUID,
        theme_id: UUID | None,
        quote: str,
        start: int | None,
        end: int | None,
    ) -> CodeAssignment:
        return CodeAssignment(
            id=uuid.uuid4(),
            document_coding_id=coding_id,
            code_id=code_id,
            theme_id=theme_id,
            quote=quote,
            start_char=start,
            end_char=end,
            quote_match_status="exact",
            confidence=0.9,
        )

    c1, c2 = codings[0].id, codings[1].id
    session.add_all([
        # Same passage under one theme: duplicate span, another duplicate, and a
        # longer overlapping span -> only the longest survives.
        _assignment(c1, code_a.id, theme.id, "manual handoffs slow", 17, 37),
        _assignment(c1, code_b.id, theme.id, "manual handoffs slow", 17, 37),
        _assignment(c1, code_a.id, theme.id, "The manual handoffs slow everyone down", 13, 51),
        # Distinct passage of the same theme -> kept.
        _assignment(c1, code_a.id, theme.id, "honestly", 53, 61),
        # Same passage but a different theme -> kept (different dedup group).
        _assignment(c1, code_a.id, other_theme.id, "manual handoffs slow", 17, 37),
        # Empty/whitespace quote -> ignored by the backfill, left untouched.
        _assignment(c1, code_a.id, theme.id, "   ", None, None),
        # Same passage in a different document coding -> kept.
        _assignment(c2, code_a.id, theme.id, "manual handoffs slow", 17, 37),
    ])
    await session.commit()
    return c1, c2, theme.id, other_theme.id


async def _quotes_for(session: AsyncSession, coding_id: UUID, theme_id: UUID) -> list[str]:
    rows = (
        await session.scalars(
            select(CodeAssignment.quote).where(
                CodeAssignment.document_coding_id == coding_id,
                CodeAssignment.theme_id == theme_id,
            )
        )
    ).all()
    return sorted(rows)


async def test_backfill_collapses_duplicate_and_overlapping_same_theme_quotes(db_session) -> None:
    c1, c2, theme_id, other_theme_id = await _seed(db_session)

    removed = await backfill_deduplicate_code_assignments(db_session)

    # The two shorter duplicate/overlapping spans of the passage are deleted.
    assert removed == 2

    theme_quotes = await _quotes_for(db_session, c1, theme_id)
    # Longest span of the passage plus the distinct quote plus the untouched
    # whitespace row remain.
    assert theme_quotes == ["   ", "The manual handoffs slow everyone down", "honestly"]

    # A duplicate span under a different theme is a different group -> kept.
    assert await _quotes_for(db_session, c1, other_theme_id) == ["manual handoffs slow"]

    # The identical passage in a different document coding is untouched.
    assert await _quotes_for(db_session, c2, theme_id) == ["manual handoffs slow"]


async def test_backfill_is_idempotent(db_session) -> None:
    await _seed(db_session)

    first = await backfill_deduplicate_code_assignments(db_session)
    second = await backfill_deduplicate_code_assignments(db_session)

    assert first == 2
    assert second == 0
