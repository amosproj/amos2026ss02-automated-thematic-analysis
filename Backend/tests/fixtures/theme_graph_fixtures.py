from __future__ import annotations

"""Dummy fixtures for theme-graph unit tests."""

from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import (
    ActorType,
    CodebookStatus,
    NodeStatus,
    ThemeLevel,
    ThemeRelationshipType,
)
from app.models import Codebook
from app.services.theme_graph import ThemeGraphService


@dataclass(slots=True, frozen=True)
class DummyThemeTreeIds:
    codebook_id: UUID
    root_experience: UUID
    root_coordination: UUID
    sub_data_access: UUID
    sub_tooling: UUID
    sub_handover: UUID
    sub_role_clarity: UUID


async def seed_dummy_theme_tree(
    session: AsyncSession,
    *,
    codebook_id: UUID | None = None,
    project_id: str = "project_dummy_theme",
    codebook_version: int = 1,
    codebook_name: str = "Dummy Theme Codebook",
    created_by: ActorType = ActorType.SYSTEM,
) -> DummyThemeTreeIds:
    """Seed a small two-root hierarchy plus one semantic cross-link."""
    service = ThemeGraphService(session, auto_commit=False)
    resolved_codebook_id = codebook_id or uuid4()
    await _ensure_codebook(
        session=session,
        codebook_id=resolved_codebook_id,
        project_id=project_id,
        version=codebook_version,
        name=codebook_name,
        created_by=created_by,
    )

    root_experience = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Developer Experience",
        description="Top-level concerns related to engineering workflow quality.",
        level=ThemeLevel.THEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        provenance="dummy-seed",
    )
    root_coordination = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Team Coordination",
        description="Top-level concerns related to team-level collaboration dynamics.",
        level=ThemeLevel.THEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        provenance="dummy-seed",
    )

    sub_data_access = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Data Access Friction",
        description="People report friction retrieving data needed for decisions.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root_experience.id,
        provenance="dummy-seed",
    )
    sub_tooling = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Tooling Drift",
        description="Teams rely on inconsistent toolchains that diverge over time.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root_experience.id,
        provenance="dummy-seed",
    )
    sub_handover = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Handover Quality",
        description="Transitions across shifts or squads lose contextual information.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root_coordination.id,
        provenance="dummy-seed",
    )
    sub_role_clarity = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Role Clarity",
        description="Ownership boundaries are ambiguous and generate rework.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root_coordination.id,
        provenance="dummy-seed",
    )

    await service.add_theme_relation(
        codebook_id=resolved_codebook_id,
        source_theme_id=sub_tooling.id,
        target_theme_id=sub_role_clarity.id,
        relationship_type=ThemeRelationshipType.RELATED_TO,
        created_by=created_by,
        provenance="dummy-seed",
    )

    await session.commit()
    return DummyThemeTreeIds(
        codebook_id=resolved_codebook_id,
        root_experience=root_experience.id,
        root_coordination=root_coordination.id,
        sub_data_access=sub_data_access.id,
        sub_tooling=sub_tooling.id,
        sub_handover=sub_handover.id,
        sub_role_clarity=sub_role_clarity.id,
    )


async def _ensure_codebook(
    session: AsyncSession,
    *,
    codebook_id: UUID,
    project_id: str,
    version: int,
    name: str,
    created_by: ActorType,
) -> None:
    stmt = select(Codebook).where(Codebook.id == codebook_id)
    codebook = (await session.execute(stmt)).scalar_one_or_none()
    if codebook is not None:
        return

    session.add(
        Codebook(
            id=codebook_id,
            project_id=project_id,
            previous_version_id=None,
            name=name,
            description="Dummy codebook for theme graph tests and local iteration.",
            research_question="How do teams describe recurring operational friction?",
            version=version,
            status=CodebookStatus.DRAFT,
            created_by=created_by,
        )
    )
    await session.flush()
