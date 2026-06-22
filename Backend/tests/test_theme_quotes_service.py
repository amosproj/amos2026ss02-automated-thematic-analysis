from __future__ import annotations

import importlib.util
import unittest
from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import Base, Codebook, CorpusDocument, Theme
from app.models.analysis import CodebookApplicationRun, DocumentCoding, ThemeAssignment
from app.services.theme_quotes import ThemeQuotesService

AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None

_CORPUS_ID = UUID("a1b2c3d4-0000-0000-0000-000000000001")


@dataclass(slots=True, frozen=True)
class _QuoteSeed:
    codebook_id: UUID
    theme_id: UUID
    run_id: UUID


async def _seed_base(session: AsyncSession, *, run_status: str = "succeeded") -> _QuoteSeed:
    """Insert the minimal rows needed to test ThemeQuotesService."""
    codebook_id = uuid4()
    theme_id = uuid4()
    run_id = uuid4()

    session.add(
        Codebook(
            id=codebook_id,
            corpus_id=_CORPUS_ID,
            name="Quote Test Codebook",
            description="Fixture",
            version=1,
            created_by="system",
        )
    )
    session.add(Theme(id=theme_id, codebook_id=codebook_id, label="Test Theme", is_active=True))
    session.add(
        CodebookApplicationRun(
            id=run_id,
            corpus_id=_CORPUS_ID,
            codebook_id=codebook_id,
            status=run_status,
        )
    )
    await session.flush()
    return _QuoteSeed(codebook_id=codebook_id, theme_id=theme_id, run_id=run_id)


async def _add_document_with_assignment(
    session: AsyncSession,
    seed: _QuoteSeed,
    *,
    is_present: bool,
    confidence: float,
    quote: str | None,
) -> None:
    doc_id = uuid4()
    coding_id = uuid4()
    session.add(
        CorpusDocument(
            id=doc_id,
            corpus_id=_CORPUS_ID,
            title=f"Interview {doc_id.hex[:6]}",
            content="...",
        )
    )
    session.add(
        DocumentCoding(
            id=coding_id,
            application_run_id=seed.run_id,
            document_id=doc_id,
            codebook_id=seed.codebook_id,
        )
    )
    session.add(
        ThemeAssignment(
            id=uuid4(),
            document_coding_id=coding_id,
            theme_id=seed.theme_id,
            is_present=is_present,
            confidence=confidence,
            quote=quote,
        )
    )


@unittest.skipUnless(AIOSQLITE_AVAILABLE, "These tests require aiosqlite.")
class ThemeQuotesServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def test_returns_empty_page_when_no_succeeded_run_exists(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_base(session, run_status="running")
            await _add_document_with_assignment(
                session, seed, is_present=True, confidence=0.9, quote="some quote"
            )
            await session.commit()

            result = await ThemeQuotesService(session).list_theme_quotes(
                codebook_id=seed.codebook_id, theme_id=seed.theme_id
            )

        self.assertEqual(result.meta.total, 0)
        self.assertEqual(result.items, [])

    async def test_excludes_absent_theme_assignments(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_base(session)
            await _add_document_with_assignment(
                session, seed, is_present=False, confidence=0.9, quote="ignored quote"
            )
            await session.commit()

            result = await ThemeQuotesService(session).list_theme_quotes(
                codebook_id=seed.codebook_id, theme_id=seed.theme_id
            )

        self.assertEqual(result.meta.total, 0)

    async def test_excludes_null_quote_assignments(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_base(session)
            await _add_document_with_assignment(
                session, seed, is_present=True, confidence=0.9, quote=None
            )
            await session.commit()

            result = await ThemeQuotesService(session).list_theme_quotes(
                codebook_id=seed.codebook_id, theme_id=seed.theme_id
            )

        self.assertEqual(result.meta.total, 0)

    async def test_returns_quotes_ordered_by_confidence_descending(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_base(session)
            for conf, text in [(0.7, "low"), (0.95, "high"), (0.82, "mid")]:
                await _add_document_with_assignment(
                    session, seed, is_present=True, confidence=conf, quote=text
                )
            await session.commit()

            result = await ThemeQuotesService(session).list_theme_quotes(
                codebook_id=seed.codebook_id, theme_id=seed.theme_id
            )

        self.assertEqual([item.quote for item in result.items], ["high", "mid", "low"])

    async def test_pagination_returns_correct_slice_and_metadata(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_base(session)
            for i in range(5):
                await _add_document_with_assignment(
                    session, seed, is_present=True, confidence=float(i) / 10, quote=f"q{i}"
                )
            await session.commit()

            result = await ThemeQuotesService(session).list_theme_quotes(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_id,
                page=2,
                page_size=2,
            )

        self.assertEqual(result.meta.total, 5)
        self.assertEqual(result.meta.pages, 3)
        self.assertEqual(result.meta.page, 2)
        self.assertEqual(len(result.items), 2)

    async def test_uses_explicit_run_id_and_ignores_latest_succeeded(self) -> None:
        async with self.session_factory() as session:
            # One succeeded run with a quote
            seed = await _seed_base(session, run_status="succeeded")
            await _add_document_with_assignment(
                session, seed, is_present=True, confidence=0.9, quote="from succeeded run"
            )

            # Second run (different id) — no assignments
            other_run_id = uuid4()
            session.add(
                CodebookApplicationRun(
                    id=other_run_id,
                    corpus_id=_CORPUS_ID,
                    codebook_id=seed.codebook_id,
                    status="succeeded",
                )
            )
            await session.commit()

            result = await ThemeQuotesService(session).list_theme_quotes(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_id,
                application_run_id=other_run_id,
            )

        # Explicit run has no assignments → empty even though another run has quotes
        self.assertEqual(result.meta.total, 0)
