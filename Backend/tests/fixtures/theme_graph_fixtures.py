from __future__ import annotations

"""Dummy fixtures for theme-graph unit tests."""

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path
import secrets
from uuid import UUID, uuid4

from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings
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
    project_id: str | None = None,
    codebook_version: int = 1,
    codebook_name: str | None = None,
    created_by: ActorType = ActorType.SYSTEM,
) -> DummyThemeTreeIds:
    """Seed a small two-root hierarchy plus one semantic cross-link."""
    service = ThemeGraphService(session, auto_commit=False)
    resolved_codebook_id = codebook_id or uuid4()
    resolved_project_id = project_id or _with_random_suffix("project_dummy_theme")
    resolved_codebook_name = codebook_name or _with_random_suffix("Dummy Theme Codebook")
    await _ensure_codebook(
        session=session,
        codebook_id=resolved_codebook_id,
        project_id=resolved_project_id,
        version=codebook_version,
        name=resolved_codebook_name,
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


async def seed_three_theme_fixture(
    session: AsyncSession,
    *,
    codebook_id: UUID | None = None,
    project_id: str | None = None,
    codebook_version: int = 1,
    codebook_name: str | None = None,
    created_by: ActorType = ActorType.SYSTEM,
) -> UUID:
    """Seed a minimal fixture with 3 themes (1 root + 2 children)."""
    service = ThemeGraphService(session, auto_commit=False)
    resolved_codebook_id = codebook_id or uuid4()
    resolved_project_id = project_id or _with_random_suffix("project_fixture_themes_3")
    resolved_codebook_name = codebook_name or _with_random_suffix("Fixture Theme Codebook (3)")
    await _ensure_codebook(
        session=session,
        codebook_id=resolved_codebook_id,
        project_id=resolved_project_id,
        version=codebook_version,
        name=resolved_codebook_name,
        created_by=created_by,
    )

    root = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Delivery Quality",
        description="Root theme for software delivery reliability.",
        level=ThemeLevel.THEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        provenance="fixture-seed-3",
    )
    await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Release Predictability",
        description="Schedules and release planning reliability.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root.id,
        provenance="fixture-seed-3",
    )
    await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Incident Recovery",
        description="Recovery speed and runbook effectiveness after incidents.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root.id,
        provenance="fixture-seed-3",
    )

    await session.commit()
    return resolved_codebook_id


async def seed_unbalanced_fifteen_theme_fixture(
    session: AsyncSession,
    *,
    codebook_id: UUID | None = None,
    project_id: str | None = None,
    codebook_version: int = 1,
    codebook_name: str | None = None,
    created_by: ActorType = ActorType.SYSTEM,
) -> UUID:
    """Seed an unbalanced tree with 15 themes for frontend hierarchy stress tests."""
    service = ThemeGraphService(session, auto_commit=False)
    resolved_codebook_id = codebook_id or uuid4()
    resolved_project_id = project_id or _with_random_suffix("project_fixture_themes_15_unbalanced")
    resolved_codebook_name = codebook_name or _with_random_suffix(
        "Fixture Theme Codebook (15 Unbalanced)"
    )
    await _ensure_codebook(
        session=session,
        codebook_id=resolved_codebook_id,
        project_id=resolved_project_id,
        version=codebook_version,
        name=resolved_codebook_name,
        created_by=created_by,
    )

    root = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Operational Friction",
        description="Top-level operational friction observed across teams.",
        level=ThemeLevel.THEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        provenance="fixture-seed-15",
    )

    # Long branch (depth-heavy): 8 nodes including root.
    long_1 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Planning Breakdown",
        description="Breakdowns in planning quality.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root.id,
        provenance="fixture-seed-15",
    )
    long_2 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Scope Volatility",
        description="Frequent requirement changes destabilize execution.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=long_1.id,
        provenance="fixture-seed-15",
    )
    long_3 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Dependency Drift",
        description="External dependencies shift without synchronized updates.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=long_2.id,
        provenance="fixture-seed-15",
    )
    long_4 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Coordination Lag",
        description="Coordination loops add latency to decisions.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=long_3.id,
        provenance="fixture-seed-15",
    )
    long_5 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Handover Loss",
        description="Context is lost across work handovers.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=long_4.id,
        provenance="fixture-seed-15",
    )
    long_6 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Escalation Ambiguity",
        description="Unclear escalation ownership delays interventions.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=long_5.id,
        provenance="fixture-seed-15",
    )
    long_7 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Retrospective Fatigue",
        description="Feedback loops weaken due to repeated unresolved issues.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=long_6.id,
        provenance="fixture-seed-15",
    )

    # Wide shallow area under root.
    shallow_a = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Tooling Fragmentation",
        description="Many overlapping tools increase cognitive load.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root.id,
        provenance="fixture-seed-15",
    )
    shallow_b = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Data Access Delays",
        description="Critical data is available too late for decisions.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root.id,
        provenance="fixture-seed-15",
    )
    shallow_c = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Review Queue Bottlenecks",
        description="Review queues block throughput in key flows.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=root.id,
        provenance="fixture-seed-15",
    )

    # Small subtree branching from shallow_a.
    branch_1 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Integration Overhead",
        description="Integration work consumes disproportionate effort.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=shallow_a.id,
        provenance="fixture-seed-15",
    )
    branch_2 = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Environment Inconsistency",
        description="Non-uniform environments cause deployment surprises.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=shallow_a.id,
        provenance="fixture-seed-15",
    )

    # Deepen one of the shallow branches.
    _ = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Access Workarounds",
        description="Teams create manual workarounds for missing access paths.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=shallow_b.id,
        provenance="fixture-seed-15",
    )
    _ = await service.create_theme(
        codebook_id=resolved_codebook_id,
        label="Approval Churn",
        description="Approvals bounce between stakeholders without closure.",
        level=ThemeLevel.SUBTHEME,
        created_by=created_by,
        status=NodeStatus.ACTIVE,
        parent_theme_id=branch_1.id,
        provenance="fixture-seed-15",
    )

    # Keep two semantic edges for graph rendering tests.
    await service.add_theme_relation(
        codebook_id=resolved_codebook_id,
        source_theme_id=long_7.id,
        target_theme_id=shallow_c.id,
        relationship_type=ThemeRelationshipType.RELATED_TO,
        created_by=created_by,
        provenance="fixture-seed-15",
    )
    await service.add_theme_relation(
        codebook_id=resolved_codebook_id,
        source_theme_id=branch_2.id,
        target_theme_id=long_3.id,
        relationship_type=ThemeRelationshipType.RELATED_TO,
        created_by=created_by,
        provenance="fixture-seed-15",
    )

    tree = await service.get_theme_tree(codebook_id=resolved_codebook_id)
    max_depth = _max_tree_depth(tree)
    if max_depth < 4:
        raise RuntimeError(
            f"Invalid 15-theme fixture: expected at least 4 levels, got {max_depth}."
        )

    await session.commit()
    return resolved_codebook_id


async def seed_theme_fixtures_for_frontend(session: AsyncSession) -> tuple[UUID, UUID]:
    """Seed both requested fixtures: 3-theme and 15-theme unbalanced trees."""
    codebook_3 = await seed_three_theme_fixture(session)
    codebook_15 = await seed_unbalanced_fifteen_theme_fixture(session)
    return codebook_3, codebook_15


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


def _build_fixture_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Seed theme fixtures into a target database.")
    parser.add_argument(
        "--target-db",
        choices=("main",),
        default="main",
        help="Database target. Use 'main' to use DATABASE_URL from environment/.env.",
    )
    parser.add_argument(
        "--fixture",
        choices=("both", "3", "15"),
        default="both",
        help="Which fixture to seed.",
    )
    return parser


def _max_tree_depth(nodes: list) -> int:
    if not nodes:
        return 0

    max_depth = 0
    stack = [(node, 1) for node in nodes]
    while stack:
        current, depth = stack.pop()
        if depth > max_depth:
            max_depth = depth
        for child in current.children:
            stack.append((child, depth + 1))
    return max_depth


def _with_random_suffix(base: str, *, suffix_len: int = 6) -> str:
    return f"{base}_{secrets.token_hex(suffix_len // 2 + suffix_len % 2)[:suffix_len]}"


async def _run_fixture_cli(*, target_db: str, fixture: str) -> None:
    if target_db != "main":
        raise ValueError(f"Unsupported target-db: {target_db}")

    # Always load Backend/.env, independent of the current working directory.
    backend_root = Path(__file__).resolve().parents[2]
    load_dotenv(backend_root / ".env", override=False)
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True, echo=settings.APP_DEBUG)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    try:
        async with session_factory() as session:
            if fixture == "3":
                codebook = await seed_three_theme_fixture(session)
                print(f"Seeded 3-theme fixture into main DB. codebook_id={codebook}")
            elif fixture == "15":
                codebook = await seed_unbalanced_fifteen_theme_fixture(session)
                print(f"Seeded 15-theme unbalanced fixture into main DB. codebook_id={codebook}")
            else:
                codebook_3, codebook_15 = await seed_theme_fixtures_for_frontend(session)
                print(
                    "Seeded both fixtures into main DB. "
                    f"codebook_id_3={codebook_3} codebook_id_15={codebook_15}"
                )
    finally:
        await engine.dispose()


def main() -> None:
    parser = _build_fixture_arg_parser()
    args = parser.parse_args()
    asyncio.run(_run_fixture_cli(target_db=args.target_db, fixture=args.fixture))


if __name__ == "__main__":
    main()
