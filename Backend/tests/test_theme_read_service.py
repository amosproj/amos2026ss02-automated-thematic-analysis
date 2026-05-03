from __future__ import annotations

import importlib.util
import unittest

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import Base
from app.services.theme_read import ThemeReadService
from tests.fixtures.theme_graph_fixtures import seed_unbalanced_dummy_tree


AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None


@unittest.skipUnless(
    AIOSQLITE_AVAILABLE,
    "These tests require aiosqlite.",
)
class ThemeReadServiceTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_returns_tree_response_for_codebook(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_unbalanced_dummy_tree(session)
            payload = await ThemeReadService(session).get_theme_tree(codebook_id=ids.codebook_id)

            self.assertEqual(payload.codebook_id, ids.codebook_id)
            self.assertEqual(len(payload.tree), 2)
