from __future__ import annotations

import importlib.util
import unittest

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.domain.enums import ActorType, NodeStatus, ThemeLevel
from app.models import Base, Theme
from app.services import ThemeGraphService, ThemeValidationError
from app.services.theme_graph import NewThemeSpec
from tests.fixtures.theme_graph_fixtures import seed_dummy_theme_tree


AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None


@unittest.skipUnless(
    AIOSQLITE_AVAILABLE,
    "These tests require aiosqlite.",
)
class ThemeGraphServiceTests(unittest.IsolatedAsyncioTestCase):
    """
    Unit tests for theme-only graph operations.

    We use an in-memory SQLite database with a static pool so every async
    session in a test method sees the same transient database state.
    """

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

    async def test_dummy_tree_shape(self) -> None:
        """The seeded dummy dataset should materialize a two-root tree."""
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)
            tree = await service.get_theme_tree(codebook_id=ids.codebook_id)

            root_labels = sorted(node.theme.label for node in tree)
            self.assertEqual(root_labels, ["Developer Experience", "Team Coordination"])

            by_label = {node.theme.label: node for node in tree}
            exp_children = sorted(child.theme.label for child in by_label["Developer Experience"].children)
            coord_children = sorted(child.theme.label for child in by_label["Team Coordination"].children)

            self.assertEqual(exp_children, ["Data Access Friction", "Tooling Drift"])
            self.assertEqual(coord_children, ["Handover Quality", "Role Clarity"])

    async def test_auto_generate_theme_tree_for_selected_codebook(self) -> None:
        """One-call API should generate the hierarchy for the selected codebook ID."""
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            tree = await service.auto_generate_theme_tree_for_codebook(codebook_id=ids.codebook_id)
            root_labels = sorted(node.theme.label for node in tree)
            self.assertEqual(root_labels, ["Developer Experience", "Team Coordination"])

    async def test_rejects_cycle_when_adding_child_edge(self) -> None:
        """Attempting to make a parent a child of its own descendant must fail."""
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            with self.assertRaises(ThemeValidationError):
                await service.add_child_theme(
                    codebook_id=ids.codebook_id,
                    parent_theme_id=ids.sub_data_access,
                    child_theme_id=ids.root_experience,
                    created_by=ActorType.HUMAN,
                    provenance="unit-test",
                )

    async def test_merge_marks_sources_and_rewires_hierarchy(self) -> None:
        """
        Merge should:
        - create a new node,
        - mark source nodes as MERGED,
        - replace source children under the parent in tree projection.
        """
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            merged = await service.merge_themes(
                codebook_id=ids.codebook_id,
                source_theme_ids=[ids.sub_data_access, ids.sub_tooling],
                merged_label="Workflow Friction",
                merged_description="Combined friction theme from tooling and access.",
                created_by=ActorType.LLM,
                merged_level=ThemeLevel.SUBTHEME,
                parent_theme_id=ids.root_experience,
                provenance="unit-test",
            )

            self.assertEqual(merged.label, "Workflow Friction")

            source_stmt = select(Theme).where(Theme.id.in_([ids.sub_data_access, ids.sub_tooling]))
            source_nodes = list((await session.scalars(source_stmt)).all())
            self.assertEqual({node.status for node in source_nodes}, {NodeStatus.MERGED})

            tree = await service.get_theme_tree(codebook_id=ids.codebook_id)
            by_label = {node.theme.label: node for node in tree}
            exp_children = sorted(child.theme.label for child in by_label["Developer Experience"].children)
            self.assertEqual(exp_children, ["Workflow Friction"])

    async def test_split_deprecates_source_and_inherits_parent(self) -> None:
        """
        Split should deprecate source and attach new themes to the original parent.

        This mirrors an iterative refinement move where one coarse theme is
        replaced by two finer-grained subthemes.
        """
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            new_nodes = await service.split_theme(
                codebook_id=ids.codebook_id,
                source_theme_id=ids.sub_handover,
                split_specs=[
                    NewThemeSpec(
                        label="Handover Documentation",
                        description="Written handover artifacts are inconsistent.",
                        level=ThemeLevel.SUBTHEME,
                        status=NodeStatus.ACTIVE,
                    ),
                    NewThemeSpec(
                        label="Handover Timing",
                        description="Timing constraints limit complete context transfer.",
                        level=ThemeLevel.SUBTHEME,
                        status=NodeStatus.ACTIVE,
                    ),
                ],
                created_by=ActorType.HUMAN,
                inherit_parent=True,
                provenance="unit-test",
            )

            self.assertEqual(len(new_nodes), 2)
            old_node = await session.get(Theme, ids.sub_handover)
            self.assertIsNotNone(old_node)
            self.assertEqual(old_node.status, NodeStatus.DEPRECATED)

            tree = await service.get_theme_tree(codebook_id=ids.codebook_id)
            by_label = {node.theme.label: node for node in tree}
            coord_children = sorted(child.theme.label for child in by_label["Team Coordination"].children)
            self.assertEqual(
                coord_children,
                ["Handover Documentation", "Handover Timing", "Role Clarity"],
            )
