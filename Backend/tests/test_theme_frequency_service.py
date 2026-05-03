from __future__ import annotations

import importlib.util
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import Base
from app.services.theme_frequency import ThemeFrequencyService
from app.services.theme_graph import ThemeNotFoundError
from tests.fixtures.theme_graph_fixtures import (
    seed_three_theme_codebook,
    seed_zero_occurrence_theme_corpus,
)


AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None


@unittest.skipUnless(
    AIOSQLITE_AVAILABLE,
    "These tests require aiosqlite.",
)
class ThemeFrequencyServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def test_returns_all_themes_with_hardcoded_zero_frequency(self) -> None:
        async with self.session_factory() as session:
            seed = await seed_zero_occurrence_theme_corpus(session)
            payload = await ThemeFrequencyService(session).list_theme_frequencies(
                codebook_id=seed.codebook_id
            )

            self.assertEqual(len(payload), len(seed.theme_ids_by_label))
            self.assertEqual({item.theme_id for item in payload}, set(seed.theme_ids_by_label.values()))
            self.assertTrue(all(item.occurrence_count == 0 for item in payload))
            self.assertTrue(all(item.interview_coverage_percentage == 0 for item in payload))

    async def test_sorts_by_frequency_desc_then_name(self) -> None:
        async with self.session_factory() as session:
            seed = await seed_three_theme_codebook(session)
            service = ThemeFrequencyService(session)

            counts_by_theme_id = {
                seed.theme_ids_by_label["Delivery Confidence"]: 2,
                seed.theme_ids_by_label["Planning Clarity"]: 5,
                seed.theme_ids_by_label["Scope Stability"]: 5,
            }
            with (
                patch.object(
                    ThemeFrequencyService,
                    "_load_occurrence_count_by_theme_id",
                    new=AsyncMock(return_value=counts_by_theme_id),
                ),
                patch.object(
                    ThemeFrequencyService,
                    "_load_total_interviews_in_corpus",
                    new=AsyncMock(return_value=10),
                ),
            ):
                payload = await service.list_theme_frequencies(codebook_id=seed.codebook_id)

            self.assertEqual(
                [item.theme_name for item in payload],
                ["Planning Clarity", "Scope Stability", "Delivery Confidence"],
            )
            self.assertEqual(
                [item.interview_coverage_percentage for item in payload],
                [50.0, 50.0, 20.0],
            )

    async def test_raises_not_found_for_unknown_codebook(self) -> None:
        async with self.session_factory() as session:
            with self.assertRaises(ThemeNotFoundError):
                await ThemeFrequencyService(session).list_theme_frequencies(codebook_id=uuid4())
