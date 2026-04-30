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
    ThemeRelationshipType,
    ThemeLevel,
)
from app.models import Base, Codebook, CodebookThemeRelationship, Theme, ThemeRelationship
from app.services import ThemeConflictError, ThemeGraphService, ThemeNotFoundError, ThemeValidationError
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

    async def test_subtheme_can_have_subtheme_child(self) -> None:
        """Nested subthemes should be valid and materialize in tree projection."""
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            nested = await service.create_theme(
                codebook_id=ids.codebook_id,
                label="Nested Subtheme",
                description="Child subtheme under an existing subtheme.",
                level=ThemeLevel.SUBTHEME,
                created_by=ActorType.SYSTEM,
                status=NodeStatus.ACTIVE,
                parent_theme_id=ids.sub_data_access,
                provenance="unit-test",
            )
            self.assertIsNotNone(nested.id)

            tree = await service.get_theme_tree(codebook_id=ids.codebook_id)
            by_root = {node.theme.label: node for node in tree}
            exp_root = by_root["Developer Experience"]
            by_child = {node.theme.label: node for node in exp_root.children}
            self.assertIn("Data Access Friction", by_child)
            nested_labels = [node.theme.label for node in by_child["Data Access Friction"].children]
            self.assertIn("Nested Subtheme", nested_labels)

    async def test_update_and_deprecate_theme(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            updated = await service.update_theme(
                codebook_id=ids.codebook_id,
                theme_id=ids.sub_data_access,
                label="Data Access Bottlenecks",
                description="Updated description",
                status=NodeStatus.ACTIVE,
            )
            self.assertEqual(updated.label, "Data Access Bottlenecks")
            self.assertEqual(updated.description, "Updated description")
            self.assertEqual(updated.status, NodeStatus.ACTIVE)

            deprecated = await service.deprecate_theme(
                codebook_id=ids.codebook_id,
                theme_id=ids.sub_tooling,
            )
            self.assertEqual(deprecated.status, NodeStatus.DEPRECATED)

    async def test_remove_child_theme_returns_count_and_removes_edge(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            removed = await service.remove_child_theme(
                codebook_id=ids.codebook_id,
                parent_theme_id=ids.root_experience,
                child_theme_id=ids.sub_data_access,
            )
            self.assertEqual(removed, 1)

            removed_again = await service.remove_child_theme(
                codebook_id=ids.codebook_id,
                parent_theme_id=ids.root_experience,
                child_theme_id=ids.sub_data_access,
            )
            self.assertEqual(removed_again, 0)

            tree = await service.get_theme_tree(codebook_id=ids.codebook_id)
            by_label = {node.theme.label: node for node in tree}
            exp_children = [child.theme.label for child in by_label["Developer Experience"].children]
            self.assertEqual(exp_children, ["Tooling Drift"])

    async def test_move_theme_reparents_and_rejects_self_parent(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            moved = await service.move_theme(
                codebook_id=ids.codebook_id,
                theme_id=ids.sub_tooling,
                new_parent_theme_id=ids.root_coordination,
                created_by=ActorType.HUMAN,
                provenance="unit-test",
            )
            self.assertEqual(moved.target_theme_id, ids.root_coordination)

            tree = await service.get_theme_tree(codebook_id=ids.codebook_id)
            by_label = {node.theme.label: node for node in tree}
            exp_children = sorted(child.theme.label for child in by_label["Developer Experience"].children)
            coord_children = sorted(child.theme.label for child in by_label["Team Coordination"].children)
            self.assertEqual(exp_children, ["Data Access Friction"])
            self.assertEqual(coord_children, ["Handover Quality", "Role Clarity", "Tooling Drift"])

            with self.assertRaises(ThemeValidationError):
                await service.move_theme(
                    codebook_id=ids.codebook_id,
                    theme_id=ids.sub_tooling,
                    new_parent_theme_id=ids.sub_tooling,
                    created_by=ActorType.HUMAN,
                    provenance="unit-test",
                )

    async def test_add_and_remove_theme_relation(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            with self.assertRaises(ThemeValidationError):
                await service.add_theme_relation(
                    codebook_id=ids.codebook_id,
                    source_theme_id=ids.sub_tooling,
                    target_theme_id=ids.sub_role_clarity,
                    relationship_type=ThemeRelationshipType.CHILD_OF,
                    created_by=ActorType.SYSTEM,
                    provenance="unit-test",
                )

            rel = await service.add_theme_relation(
                codebook_id=ids.codebook_id,
                source_theme_id=ids.sub_handover,
                target_theme_id=ids.sub_data_access,
                relationship_type=ThemeRelationshipType.EQUIVALENT_TO,
                created_by=ActorType.SYSTEM,
                provenance="unit-test",
            )
            self.assertEqual(rel.relationship_type, ThemeRelationshipType.EQUIVALENT_TO)

            removed = await service.remove_theme_relation(
                codebook_id=ids.codebook_id,
                source_theme_id=ids.sub_handover,
                target_theme_id=ids.sub_data_access,
                relationship_type=ThemeRelationshipType.EQUIVALENT_TO,
            )
            self.assertEqual(removed, 1)

            removed_again = await service.remove_theme_relation(
                codebook_id=ids.codebook_id,
                source_theme_id=ids.sub_handover,
                target_theme_id=ids.sub_data_access,
                relationship_type=ThemeRelationshipType.EQUIVALENT_TO,
            )
            self.assertEqual(removed_again, 0)

    async def test_delete_theme_soft_removes_membership_and_edges(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            await service.delete_theme(
                codebook_id=ids.codebook_id,
                theme_id=ids.sub_role_clarity,
                hard=False,
            )

            deleted = await session.get(Theme, ids.sub_role_clarity)
            self.assertIsNotNone(deleted)
            self.assertEqual(deleted.status, NodeStatus.DELETED)

            membership_stmt = select(CodebookThemeRelationship).where(
                CodebookThemeRelationship.codebook_id == ids.codebook_id,
                CodebookThemeRelationship.theme_id == ids.sub_role_clarity,
                CodebookThemeRelationship.relationship_type == CodebookThemeRelationshipType.CONTAINS,
            )
            memberships = list((await session.scalars(membership_stmt)).all())
            self.assertTrue(memberships)
            self.assertEqual({row.status for row in memberships}, {RelationshipStatus.REMOVED})

            edge_stmt = select(ThemeRelationship).where(
                ThemeRelationship.codebook_id == ids.codebook_id,
                ThemeRelationship.status == RelationshipStatus.ACTIVE,
                (
                    (ThemeRelationship.source_theme_id == ids.sub_role_clarity)
                    | (ThemeRelationship.target_theme_id == ids.sub_role_clarity)
                ),
            )
            self.assertEqual(list((await session.scalars(edge_stmt)).all()), [])

    async def test_delete_theme_hard_deletes_single_membership_theme(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            leaf = await service.create_theme(
                codebook_id=ids.codebook_id,
                label="Disposable Leaf",
                description="Temporary leaf for hard-delete test.",
                level=ThemeLevel.SUBTHEME,
                created_by=ActorType.SYSTEM,
                status=NodeStatus.ACTIVE,
                parent_theme_id=ids.root_experience,
                provenance="unit-test",
            )
            await service.delete_theme(codebook_id=ids.codebook_id, theme_id=leaf.id, hard=True)
            self.assertIsNone(await session.get(Theme, leaf.id))

    async def test_delete_theme_hard_raises_when_theme_is_shared(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            codebook_v2_id = uuid4()
            session.add(
                Codebook(
                    id=codebook_v2_id,
                    project_id="project_shared_theme",
                    previous_version_id=ids.codebook_id,
                    name="Shared Theme Codebook v2",
                    description="codebook",
                    version=2,
                    status=CodebookStatus.DRAFT,
                    created_by=ActorType.SYSTEM,
                )
            )
            session.add(
                CodebookThemeRelationship(
                    id=uuid4(),
                    codebook_id=codebook_v2_id,
                    theme_id=ids.sub_tooling,
                    relationship_type=CodebookThemeRelationshipType.CONTAINS,
                    status=RelationshipStatus.ACTIVE,
                    created_by=ActorType.SYSTEM,
                    provenance="unit-test",
                )
            )
            await session.commit()

            with self.assertRaises(ThemeConflictError):
                await service.delete_theme(
                    codebook_id=ids.codebook_id,
                    theme_id=ids.sub_tooling,
                    hard=True,
                )

    async def test_split_theme_requires_at_least_two_specs(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)
            with self.assertRaises(ThemeValidationError):
                await service.split_theme(
                    codebook_id=ids.codebook_id,
                    source_theme_id=ids.sub_handover,
                    split_specs=[
                        NewThemeSpec(
                            label="Only one split",
                            description="invalid",
                            level=ThemeLevel.SUBTHEME,
                            status=NodeStatus.ACTIVE,
                        )
                    ],
                    created_by=ActorType.SYSTEM,
                    inherit_parent=True,
                    provenance="unit-test",
                )

    async def test_merge_themes_raises_for_ambiguous_parent_without_override(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            with self.assertRaises(ThemeValidationError):
                await service.merge_themes(
                    codebook_id=ids.codebook_id,
                    source_theme_ids=[ids.sub_data_access, ids.sub_handover],
                    merged_label="Ambiguous Merge",
                    merged_description="merge from different roots",
                    created_by=ActorType.SYSTEM,
                    provenance="unit-test",
                )

    async def test_replace_theme_rewires_edges_and_deprecates_old(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            replacement = await service.replace_theme(
                codebook_id=ids.codebook_id,
                old_theme_id=ids.sub_tooling,
                new_theme_spec=NewThemeSpec(
                    label="Tooling Misalignment",
                    description="Replacement for tooling drift.",
                    level=ThemeLevel.SUBTHEME,
                    status=NodeStatus.ACTIVE,
                ),
                created_by=ActorType.HUMAN,
                provenance="unit-test",
            )

            old_theme = await session.get(Theme, ids.sub_tooling)
            self.assertIsNotNone(old_theme)
            self.assertEqual(old_theme.status, NodeStatus.DEPRECATED)

            tree = await service.get_theme_tree(codebook_id=ids.codebook_id)
            by_label = {node.theme.label: node for node in tree}
            exp_children = sorted(child.theme.label for child in by_label["Developer Experience"].children)
            self.assertEqual(exp_children, ["Data Access Friction", "Tooling Misalignment"])

            related_stmt = select(ThemeRelationship).where(
                ThemeRelationship.codebook_id == ids.codebook_id,
                ThemeRelationship.source_theme_id == replacement.id,
                ThemeRelationship.target_theme_id == ids.sub_role_clarity,
                ThemeRelationship.relationship_type == ThemeRelationshipType.RELATED_TO,
                ThemeRelationship.status == RelationshipStatus.ACTIVE,
            )
            self.assertIsNotNone((await session.execute(related_stmt)).scalar_one_or_none())

    async def test_replace_theme_requires_parent_override_when_old_theme_has_multiple_parents(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session, auto_commit=False)

            session.add(
                ThemeRelationship(
                    id=uuid4(),
                    codebook_id=ids.codebook_id,
                    source_theme_id=ids.sub_tooling,
                    target_theme_id=ids.root_coordination,
                    relationship_type=ThemeRelationshipType.CHILD_OF,
                    status=RelationshipStatus.ACTIVE,
                    created_by=ActorType.SYSTEM,
                    provenance="unit-test-manual",
                )
            )
            await session.commit()

            with self.assertRaises(ThemeValidationError):
                await service.replace_theme(
                    codebook_id=ids.codebook_id,
                    old_theme_id=ids.sub_tooling,
                    new_theme_spec=NewThemeSpec(
                        label="Replacement",
                        description="Replacement with ambiguous parent",
                        level=ThemeLevel.SUBTHEME,
                        status=NodeStatus.ACTIVE,
                    ),
                    created_by=ActorType.SYSTEM,
                    provenance="unit-test",
                )

    async def test_validate_theme_dag_and_build_theme_dag_branches(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session, auto_commit=False)

            # A level violation: a THEME node should not be a child node.
            await service.create_theme(
                codebook_id=ids.codebook_id,
                label="Invalid Child Theme-Level Node",
                description="Invalid hierarchy level test",
                level=ThemeLevel.THEME,
                created_by=ActorType.SYSTEM,
                status=NodeStatus.ACTIVE,
                parent_theme_id=ids.root_experience,
                provenance="unit-test",
            )
            validation = await service.validate_theme_dag(codebook_id=ids.codebook_id)
            self.assertFalse(validation.is_valid)
            self.assertTrue(any("cannot be child_of" in violation for violation in validation.violations))

            dag = await service.build_theme_dag(
                codebook_id=ids.codebook_id,
                include_non_hierarchical=False,
            )
            all_rel_types = {
                edge.relationship_type
                for edges in dag.adjacency.values()
                for edge in edges
            }
            self.assertEqual(all_rel_types, {ThemeRelationshipType.CHILD_OF})

    async def test_get_theme_tree_unknown_root_raises(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)
            with self.assertRaises(ThemeNotFoundError):
                await service.get_theme_tree(
                    codebook_id=ids.codebook_id,
                    root_theme_id=uuid4(),
                )

    async def test_auto_generate_excludes_candidate_nodes_when_requested(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_dummy_theme_tree(session)
            service = ThemeGraphService(session)

            await service.create_theme(
                codebook_id=ids.codebook_id,
                label="Candidate Child",
                description="Candidate node for filtering test",
                level=ThemeLevel.SUBTHEME,
                created_by=ActorType.SYSTEM,
                status=NodeStatus.CANDIDATE,
                parent_theme_id=ids.root_experience,
                provenance="unit-test",
            )

            with_candidates = await service.auto_generate_theme_tree_for_codebook(
                codebook_id=ids.codebook_id,
                include_candidate_nodes=True,
            )
            without_candidates = await service.auto_generate_theme_tree_for_codebook(
                codebook_id=ids.codebook_id,
                include_candidate_nodes=False,
            )

            by_label_with = {node.theme.label: node for node in with_candidates}
            by_label_without = {node.theme.label: node for node in without_candidates}
            with_child_labels = {child.theme.label for child in by_label_with["Developer Experience"].children}
            without_child_labels = {
                child.theme.label for child in by_label_without["Developer Experience"].children
            }
            self.assertIn("Candidate Child", with_child_labels)
            self.assertNotIn("Candidate Child", without_child_labels)
