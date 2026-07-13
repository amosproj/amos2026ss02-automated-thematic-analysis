"""Seed a demo corpus that shows overlapping code quotes in the read view.

Six transcripts, one succeeded application run, covering:
  * same-theme overlaps  — two or three codes of ONE theme nested on a passage
    (rendered as darker / stacked highlight bands),
  * cross-theme overlaps — codes of DIFFERENT themes nested on a passage,
  * non-overlapping codes — for visual contrast.

Overlaps are containment (inner quote is a substring of the outer quote) because
the read view can only stack highlights when one span fully contains the other.

Idempotent: re-running first removes the prior demo corpus (matched by name),
which cascades to its codebook, run, codings and assignments. Run it from a
source checkout with the DB reachable, e.g.:

    docker compose run --rm -v "$PWD/Backend/scripts:/app/scripts" api \
        python scripts/seed_demo_overlaps.py
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

# Add Backend root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import _get_engine
from app.models import (
    Code,
    CodeAssignment,
    Codebook,
    CodebookApplicationRun,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Corpus,
    CorpusDocument,
    DocumentCoding,
    Theme,
    ThemeAssignment,
    ThemeCodeRelationship,
)

DEMO_CORPUS_NAME = "DEMO - Overlapping Quotes"

# theme label -> its code labels
THEMES: dict[str, list[str]] = {
    "Workflow Friction": ["Manual Handoffs", "Communication Gaps", "Process Delays"],
    "Team Morale": ["Burnout", "Low Motivation"],
    "Tooling": ["Legacy Systems", "Helpful Automation"],
    "Positive Experience": ["Team Support", "Clear Goals"],
}

# (title, transcript, [(code_label, quote), ...]).
# Quotes must be verbatim substrings of the transcript; for an overlap the inner
# quote must be a substring of the outer quote so the read view nests them.
DOCUMENTS: list[tuple[str, str, list[tuple[str, str]]]] = [
    (
        "Interview 1 - Handoffs (same-theme overlap x2)",
        "Interviewer: How do handoffs work on your team?\n"
        "Participant: The manual handoffs slow everyone down because nobody "
        "communicates the current status, and that is a real problem for us.",
        [
            (
                "Manual Handoffs",
                "The manual handoffs slow everyone down because nobody "
                "communicates the current status",
            ),
            ("Communication Gaps", "nobody communicates the current status"),
        ],
    ),
    (
        "Interview 2 - Releases (same-theme overlap x3)",
        "Interviewer: What slows releases down?\n"
        "Participant: Honestly, the whole release process drags because manual "
        "handoffs pile up and no one shares status between teams.",
        [
            (
                "Process Delays",
                "the whole release process drags because manual handoffs pile "
                "up and no one shares status between teams",
            ),
            ("Manual Handoffs", "manual handoffs pile up"),
            ("Communication Gaps", "no one shares status between teams"),
        ],
    ),
    (
        "Interview 3 - Morale (cross-theme overlap)",
        "Interviewer: How does that affect the team?\n"
        "Participant: We lose track of things constantly and that really hurts "
        "team morale and motivation over the long run.",
        [
            (
                "Process Delays",
                "We lose track of things constantly and that really hurts team "
                "morale and motivation",
            ),
            ("Low Motivation", "hurts team morale and motivation"),
        ],
    ),
    (
        "Interview 4 - Tooling (cross-theme overlap)",
        "Interviewer: What about the tools?\n"
        "Participant: The legacy systems are painful and the manual workarounds "
        "burn everyone out by Friday.",
        [
            ("Legacy Systems", "The legacy systems are painful and the manual workarounds"),
            ("Manual Handoffs", "the manual workarounds"),
            ("Burnout", "burn everyone out by Friday"),
        ],
    ),
    (
        "Interview 5 - Burnout (same-theme overlap)",
        "Interviewer: How are people feeling?\n"
        "Participant: People are burning out and losing motivation because the "
        "workload never lets up these days.",
        [
            (
                "Burnout",
                "People are burning out and losing motivation because the "
                "workload never lets up",
            ),
            ("Low Motivation", "losing motivation because the workload never lets up"),
        ],
    ),
    (
        "Interview 6 - Positives (same-theme overlap + standalone)",
        "Interviewer: Anything going well?\n"
        "Participant: The new automation is genuinely helpful and strong team "
        "support keeps our goals clear.",
        [
            ("Helpful Automation", "The new automation is genuinely helpful"),
            ("Team Support", "strong team support keeps our goals clear"),
            ("Clear Goals", "keeps our goals clear"),
        ],
    ),
    (
        "Interview 7 - Partial overlap (same theme, offset)",
        "Interviewer: Anything else worth mentioning?\n"
        "Participant: the manual handoffs slow everyone down and nobody "
        "communicates the status clearly enough.",
        [
            # These two spans partially overlap on 'nobody communicates' but each
            # has a distinct section -> exercises the offset-overlap render path.
            ("Manual Handoffs", "the manual handoffs slow everyone down and nobody communicates"),
            ("Communication Gaps", "nobody communicates the status clearly enough"),
        ],
    ),
]


def _naive_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


async def _delete_existing_demo(session: AsyncSession) -> None:
    corpora = list(
        (await session.scalars(select(Corpus).where(Corpus.name == DEMO_CORPUS_NAME))).all()
    )
    for corpus in corpora:
        # Every downstream FK is ON DELETE CASCADE, so deleting the corpus row
        # clears its codebook, run, codings and assignments.
        await session.delete(corpus)
    if corpora:
        await session.commit()


async def seed(session: AsyncSession) -> dict[str, object]:
    await _delete_existing_demo(session)

    corpus = Corpus(id=uuid.uuid4(), project_id=uuid.uuid4(), name=DEMO_CORPUS_NAME)
    codebook = Codebook(
        id=uuid.uuid4(),
        corpus_id=corpus.id,
        name="DEMO Codebook",
        description="Demo codebook for overlapping-quote highlighting.",
        version=1,
        created_by="seed-script",
    )
    # These models declare no ORM relationships, so the unit of work will not
    # order inserts by FK on its own: flush each parent before adding rows that
    # reference it.
    session.add(corpus)
    await session.flush()
    session.add(codebook)
    await session.flush()

    code_by_label: dict[str, Code] = {}
    theme_id_by_code: dict[str, uuid.UUID] = {}
    for theme_label, code_labels in THEMES.items():
        theme = Theme(
            id=uuid.uuid4(),
            codebook_id=codebook.id,
            label=theme_label,
            description=f"{theme_label} (demo).",
            is_active=True,
        )
        session.add(theme)
        await session.flush()
        session.add(
            CodebookThemeRelationship(
                id=uuid.uuid4(), codebook_id=codebook.id, theme_id=theme.id, is_active=True
            )
        )
        for code_label in code_labels:
            code = Code(
                id=uuid.uuid4(),
                codebook_id=codebook.id,
                label=code_label,
                description=f"{code_label} (demo).",
                is_active=True,
            )
            code_by_label[code_label] = code
            theme_id_by_code[code_label] = theme.id
            session.add(code)
            await session.flush()
            session.add(
                CodebookCodeRelationship(
                    id=uuid.uuid4(), codebook_id=codebook.id, code_id=code.id, is_active=True
                )
            )
            session.add(
                ThemeCodeRelationship(
                    id=uuid.uuid4(),
                    codebook_id=codebook.id,
                    theme_id=theme.id,
                    code_id=code.id,
                    is_active=True,
                )
            )

    await session.flush()

    run = CodebookApplicationRun(
        id=uuid.uuid4(),
        name="DEMO Overlap Run",
        corpus_id=corpus.id,
        codebook_id=codebook.id,
        status="succeeded",
        documents_total=len(DOCUMENTS),
        documents_coded=len(DOCUMENTS),
        documents_failed=0,
        started_at=_naive_now(),
        finished_at=_naive_now(),
    )
    session.add(run)
    await session.flush()

    document_ids: list[uuid.UUID] = []
    for title, content, assignments in DOCUMENTS:
        document = CorpusDocument(
            id=uuid.uuid4(), corpus_id=corpus.id, title=title, content=content
        )
        session.add(document)
        document_ids.append(document.id)
        await session.flush()

        coding = DocumentCoding(
            id=uuid.uuid4(),
            application_run_id=run.id,
            document_id=document.id,
            codebook_id=codebook.id,
            status="coded",
            summary="Demo coding with overlapping quotes.",
        )
        session.add(coding)
        await session.flush()

        seen_theme_ids: set[uuid.UUID] = set()
        for code_label, quote in assignments:
            start = content.find(quote)
            if start < 0:
                raise SystemExit(f"Quote not found verbatim in {title!r}: {quote!r}")
            end = start + len(quote)
            theme_id = theme_id_by_code[code_label]
            session.add(
                CodeAssignment(
                    id=uuid.uuid4(),
                    document_coding_id=coding.id,
                    code_id=code_by_label[code_label].id,
                    theme_id=theme_id,
                    quote=quote,
                    start_char=start,
                    end_char=end,
                    quote_match_status="exact",
                    confidence=0.9,
                    rationale=f"Demo assignment for {code_label}.",
                )
            )
            if theme_id not in seen_theme_ids:
                seen_theme_ids.add(theme_id)
                session.add(
                    ThemeAssignment(
                        id=uuid.uuid4(),
                        document_coding_id=coding.id,
                        theme_id=theme_id,
                        is_present=True,
                        confidence=0.9,
                        quote=quote,
                        start_char=start,
                        end_char=end,
                        quote_match_status="exact",
                    )
                )

    await session.commit()
    return {
        "corpus_id": corpus.id,
        "codebook_id": codebook.id,
        "run_id": run.id,
        "document_ids": document_ids,
    }


async def main() -> None:
    engine = _get_engine()
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        info = await seed(session)

    corpus_id = info["corpus_id"]
    run_id = info["run_id"]
    document_ids = info["document_ids"]
    assert isinstance(document_ids, list)
    print("Seeded demo corpus:")
    print(f"  corpus_id   = {corpus_id}")
    print(f"  codebook_id = {info['codebook_id']}")
    print(f"  run_id      = {run_id}")
    print("Read-view links (frontend on :3000):")
    for index, document_id in enumerate(document_ids, start=1):
        print(
            f"  doc {index}: "
            f"http://localhost:3000/transcripts/{corpus_id}/{document_id}/read?run_id={run_id}"
        )


if __name__ == "__main__":
    asyncio.run(main())
