from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Codebook, CodebookThemeRelationship, Theme, ThemeHierarchyRelationship


@dataclass(slots=True, frozen=True)
class DummyTreeIds:
    codebook_id: UUID
    root_product: UUID
    root_team: UUID
    sub_release: UUID
    sub_incident: UUID
    sub_handover: UUID
    leaf_playbook: UUID


async def seed_unbalanced_dummy_tree(session: AsyncSession) -> DummyTreeIds:
    """Seed a codebook with two roots and an unbalanced theme tree."""
    codebook_id = uuid4()
    root_product = uuid4()
    root_team = uuid4()
    sub_release = uuid4()
    sub_incident = uuid4()
    sub_handover = uuid4()
    leaf_playbook = uuid4()

    session.add(
        Codebook(
            id=codebook_id,
            project_id="project_theme_tree",
            name="Theme Tree Codebook",
            description="Fixture",
            version=1,
            created_by="system",
        )
    )

    for theme_id, label in [
        (root_product, "Product Delivery"),
        (root_team, "Team Coordination"),
        (sub_release, "Release Predictability"),
        (sub_incident, "Incident Recovery"),
        (sub_handover, "Handover Quality"),
        (leaf_playbook, "Playbook Quality"),
    ]:
        session.add(Theme(id=theme_id, label=label, is_active=True))
        session.add(
            CodebookThemeRelationship(
                id=uuid4(),
                codebook_id=codebook_id,
                theme_id=theme_id,
                is_active=True,
            )
        )

    # Unbalanced:
    # Product Delivery -> Release Predictability
    # Product Delivery -> Incident Recovery -> Playbook Quality
    # Team Coordination -> Handover Quality
    for parent_id, child_id in [
        (root_product, sub_release),
        (root_product, sub_incident),
        (sub_incident, leaf_playbook),
        (root_team, sub_handover),
    ]:
        session.add(
            ThemeHierarchyRelationship(
                id=uuid4(),
                codebook_id=codebook_id,
                parent_theme_id=parent_id,
                child_theme_id=child_id,
                is_active=True,
            )
        )

    await session.commit()
    return DummyTreeIds(
        codebook_id=codebook_id,
        root_product=root_product,
        root_team=root_team,
        sub_release=sub_release,
        sub_incident=sub_incident,
        sub_handover=sub_handover,
        leaf_playbook=leaf_playbook,
    )
