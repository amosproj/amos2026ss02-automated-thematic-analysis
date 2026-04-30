from __future__ import annotations

import importlib.util
import unittest
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import Base
from app.services.theme_read import ThemeReadService
from tests.fixtures.theme_graph_fixtures import seed_dummy_theme_tree


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

    async def test_resolves_latest_version_by_default(self) -> None:
        async with self.session_factory() as session:
            await seed_dummy_theme_tree(
                session,
                codebook_id=uuid4(),
                project_id="project_a",
                codebook_version=1,
                codebook_name="Project A Codebook v1",
            )
            ids_v2 = await seed_dummy_theme_tree(
                session,
                codebook_id=uuid4(),
                project_id="project_a",
                codebook_version=2,
                codebook_name="Project A Codebook v2",
            )
            service = ThemeReadService(session)

            tree_payload = await service.get_theme_tree_for_project(project_id="project_a")
            self.assertEqual(tree_payload.codebook.codebook_id, ids_v2.codebook_id)
            self.assertEqual(tree_payload.codebook.codebook_version, 2)

    async def test_resolves_explicit_version(self) -> None:
        async with self.session_factory() as session:
            ids_v1 = await seed_dummy_theme_tree(
                session,
                codebook_id=uuid4(),
                project_id="project_b",
                codebook_version=1,
                codebook_name="Project B Codebook v1",
            )
            await seed_dummy_theme_tree(
                session,
                codebook_id=uuid4(),
                project_id="project_b",
                codebook_version=2,
                codebook_name="Project B Codebook v2",
            )
            service = ThemeReadService(session)

            tree_payload = await service.get_theme_tree_for_project(project_id="project_b", version=1)
            self.assertEqual(tree_payload.codebook.codebook_id, ids_v1.codebook_id)
            self.assertEqual(tree_payload.codebook.codebook_version, 1)

    async def test_theme_frequency_payload_has_placeholders_and_sorted_output(self) -> None:
        async with self.session_factory() as session:
            await seed_dummy_theme_tree(
                session,
                codebook_id=uuid4(),
                project_id="project_c",
                codebook_version=1,
            )
            service = ThemeReadService(session)

            payload = await service.get_theme_frequency_for_project(project_id="project_c")
            self.assertEqual(payload.total_interviews_in_corpus, 0)
            self.assertEqual(len(payload.themes), 6)

            # All metrics are explicit zero placeholders until interview-theme
            # mappings are implemented.
            for item in payload.themes:
                self.assertEqual(item.occurrence_count, 0)
                self.assertEqual(item.interview_coverage_percentage, 0.0)

            # With equal frequencies, deterministic secondary sort is label asc.
            names = [item.theme_name for item in payload.themes]
            self.assertEqual(names, sorted(names, key=str.lower))
