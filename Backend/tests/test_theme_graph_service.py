from __future__ import annotations

import importlib.util
import unittest
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import Base, ThemeHierarchyRelationship
from app.services.theme_graph import ThemeGraphService, ThemeNotFoundError, ThemeValidationError
from tests.fixtures.theme_graph_fixtures import seed_unbalanced_dummy_tree


AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None


@unittest.skipUnless(
    AIOSQLITE_AVAILABLE,
    "These tests require aiosqlite.",
)
class ThemeGraphServiceTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_reads_complete_tree_from_db(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_unbalanced_dummy_tree(session)
            service = ThemeGraphService(session)

            tree = await service.get_theme_tree(codebook_id=ids.codebook_id)
            root_labels = sorted(node.theme.label for node in tree)
            self.assertEqual(root_labels, ["Product Delivery", "Team Coordination"])

            by_label = {node.theme.label: node for node in tree}
            product_children = [child.theme.label for child in by_label["Product Delivery"].children]
            self.assertEqual(product_children, ["Incident Recovery", "Release Predictability"])

            incident_children = [
                child.theme.label for child in by_label["Product Delivery"].children[0].children
            ]
            self.assertEqual(incident_children, ["Playbook Quality"])

    async def test_reads_subtree_by_root_theme_id(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_unbalanced_dummy_tree(session)
            service = ThemeGraphService(session)

            subtree = await service.get_theme_tree(
                codebook_id=ids.codebook_id,
                root_theme_id=ids.sub_incident,
            )
            self.assertEqual(len(subtree), 1)
            self.assertEqual(subtree[0].theme.label, "Incident Recovery")
            self.assertEqual([child.theme.label for child in subtree[0].children], ["Playbook Quality"])

    async def test_rejects_unknown_root(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_unbalanced_dummy_tree(session)
            service = ThemeGraphService(session)
            with self.assertRaises(ThemeNotFoundError):
                await service.get_theme_tree(codebook_id=ids.codebook_id, root_theme_id=uuid4())

    async def test_detects_cycle(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_unbalanced_dummy_tree(session)
            # Create cycle: Product Delivery <- Incident Recovery <- Playbook Quality <- Product Delivery
            session.add(
                ThemeHierarchyRelationship(
                    id=uuid4(),
                    codebook_id=ids.codebook_id,
                    parent_theme_id=ids.leaf_playbook,
                    child_theme_id=ids.root_product,
                    is_active=True,
                )
            )
            await session.commit()
            service = ThemeGraphService(session)

            validation = await service.validate_theme_dag(codebook_id=ids.codebook_id)
            self.assertFalse(validation.is_valid)
            self.assertTrue(any("cycle" in violation.lower() for violation in validation.violations))

            with self.assertRaises(ThemeValidationError):
                await service.get_theme_tree(codebook_id=ids.codebook_id)

    async def test_detects_multiple_active_parents_for_one_child(self) -> None:
        async with self.session_factory() as session:
            ids = await seed_unbalanced_dummy_tree(session)
            session.add(
                ThemeHierarchyRelationship(
                    id=uuid4(),
                    codebook_id=ids.codebook_id,
                    parent_theme_id=ids.root_team,
                    child_theme_id=ids.sub_incident,
                    is_active=True,
                )
            )
            await session.commit()
            service = ThemeGraphService(session)

            validation = await service.validate_theme_dag(codebook_id=ids.codebook_id)
            self.assertFalse(validation.is_valid)
            self.assertTrue(
                any("multiple active parents" in violation.lower() for violation in validation.violations)
            )

            with self.assertRaises(ThemeValidationError):
                await service.get_theme_tree(codebook_id=ids.codebook_id)
