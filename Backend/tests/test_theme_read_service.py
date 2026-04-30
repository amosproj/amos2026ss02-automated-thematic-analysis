from __future__ import annotations

import importlib.util
import unittest
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.domain.enums import (
    ActorType,
    CodebookStatus,
    CodebookThemeRelationshipType,
    NodeStatus,
    RelationshipStatus,
    ThemeLevel,
    ThemeRelationshipType,
)
from app.models import Base
from app.models import Codebook, CodebookThemeRelationship
from app.services.theme_read import ThemeReadService
from app.services import ThemeGraphService
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

    async def test_previous_version_tree_is_stable_when_theme_is_reused_in_newer_version(self) -> None:
        """
        Reusing the same theme_id in a newer codebook must not mutate the
        previous version's hierarchy projection.
        """
        async with self.session_factory() as session:
            ids_v1 = await seed_dummy_theme_tree(
                session,
                codebook_id=uuid4(),
                project_id="project_version_restore",
                codebook_version=1,
                codebook_name="Project Restore v1",
            )
            codebook_v2_id = uuid4()
            session.add(
                Codebook(
                    id=codebook_v2_id,
                    project_id="project_version_restore",
                    previous_version_id=ids_v1.codebook_id,
                    name="Project Restore v2",
                    description="Follow-up version with reused themes.",
                    research_question="How do themes evolve across codebook versions?",
                    version=2,
                    status=CodebookStatus.DRAFT,
                    created_by=ActorType.SYSTEM,
                )
            )
            session.add(
                CodebookThemeRelationship(
                    id=uuid4(),
                    codebook_id=codebook_v2_id,
                    theme_id=ids_v1.sub_tooling,
                    relationship_type=CodebookThemeRelationshipType.CONTAINS,
                    status=RelationshipStatus.ACTIVE,
                    created_by=ActorType.SYSTEM,
                    provenance="unit-test-reuse-theme",
                )
            )
            await session.flush()

            graph_service = ThemeGraphService(session, auto_commit=False)
            v2_root = await graph_service.create_theme(
                codebook_id=codebook_v2_id,
                label="Version 2 Root",
                description="Root in v2 that reuses a v1 theme as child.",
                level=ThemeLevel.THEME,
                created_by=ActorType.SYSTEM,
                status=NodeStatus.ACTIVE,
                provenance="unit-test-v2",
            )
            await graph_service.add_child_theme(
                codebook_id=codebook_v2_id,
                parent_theme_id=v2_root.id,
                child_theme_id=ids_v1.sub_tooling,
                created_by=ActorType.SYSTEM,
                provenance="unit-test-v2",
            )
            v2_new_theme = await graph_service.create_theme(
                codebook_id=codebook_v2_id,
                label="Version 2 New Theme",
                description="New theme linked only in v2.",
                level=ThemeLevel.SUBTHEME,
                created_by=ActorType.SYSTEM,
                status=NodeStatus.ACTIVE,
                parent_theme_id=v2_root.id,
                provenance="unit-test-v2",
            )
            await graph_service.add_theme_relation(
                codebook_id=codebook_v2_id,
                source_theme_id=ids_v1.sub_tooling,
                target_theme_id=v2_new_theme.id,
                relationship_type=ThemeRelationshipType.RELATED_TO,
                created_by=ActorType.SYSTEM,
                provenance="unit-test-v2",
            )
            await session.commit()

            service = ThemeReadService(session)

            v1_tree = await service.get_theme_tree_for_project(
                project_id="project_version_restore",
                version=1,
            )
            self.assertEqual(v1_tree.codebook.codebook_id, ids_v1.codebook_id)
            self.assertEqual(v1_tree.codebook.codebook_version, 1)

            v1_roots = {node.theme.label: node for node in v1_tree.tree}
            self.assertIn("Developer Experience", v1_roots)
            v1_dev_children = sorted(child.theme.label for child in v1_roots["Developer Experience"].children)
            self.assertEqual(v1_dev_children, ["Data Access Friction", "Tooling Drift"])

            v2_tree = await service.get_theme_tree_for_project(
                project_id="project_version_restore",
                version=2,
            )
            self.assertEqual(v2_tree.codebook.codebook_id, codebook_v2_id)
            self.assertEqual(v2_tree.codebook.codebook_version, 2)

            v2_roots = {node.theme.label: node for node in v2_tree.tree}
            self.assertIn("Version 2 Root", v2_roots)
            v2_root_children = sorted(child.theme.label for child in v2_roots["Version 2 Root"].children)
            self.assertEqual(v2_root_children, ["Tooling Drift", "Version 2 New Theme"])
