from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

from dotenv import dotenv_values
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import Codebook, CodebookThemeRelationship, Theme, ThemeHierarchyRelationship


# ============================================================================
# PYTEST FIXTURE BUILDERS (safe for test DB sessions)
# ============================================================================
# Keep this section free of direct engine creation against app settings.
# Pytest should pass in the session/engine it controls (typically in-memory DB).


@dataclass(slots=True, frozen=True)
class DummyTreeIds:
    codebook_id: UUID
    root_product: UUID
    root_team: UUID
    sub_release: UUID
    sub_incident: UUID
    sub_handover: UUID
    leaf_playbook: UUID


@dataclass(slots=True, frozen=True)
class ThemeFixtureSeed:
    codebook_id: UUID
    theme_ids_by_label: dict[str, UUID]
    occurrence_count_by_theme_id: dict[UUID, int]

    @property
    def zero_occurrence_theme_ids(self) -> tuple[UUID, ...]:
        zero_ids = [
            theme_id
            for theme_id, occurrence_count in self.occurrence_count_by_theme_id.items()
            if occurrence_count == 0
        ]
        return tuple(sorted(zero_ids, key=str))


async def _seed_codebook_with_theme_graph(
    session: AsyncSession,
    *,
    project_id: str,
    name: str,
    theme_labels: list[str],
    edges_by_label: list[tuple[str, str]],
    occurrence_count_by_label: dict[str, int] | None = None,
) -> ThemeFixtureSeed:
    codebook_id = uuid4()
    theme_ids_by_label = {label: uuid4() for label in theme_labels}

    # Insert parent tables first and flush so FK-dependent rows can be inserted
    # safely on PostgreSQL (strict FK enforcement).
    session.add(
        Codebook(
            id=codebook_id,
            project_id=project_id,
            name=name,
            description="Fixture",
            version=1,
            created_by="system",
        )
    )

    for label, theme_id in theme_ids_by_label.items():
        session.add(Theme(id=theme_id, label=label, is_active=True))

    await session.flush()

    for label, theme_id in theme_ids_by_label.items():
        session.add(
            CodebookThemeRelationship(
                id=uuid4(),
                codebook_id=codebook_id,
                theme_id=theme_id,
                is_active=True,
            )
        )

    for parent_label, child_label in edges_by_label:
        if parent_label not in theme_ids_by_label:
            raise ValueError(f"Unknown parent label in fixture edge: '{parent_label}'.")
        if child_label not in theme_ids_by_label:
            raise ValueError(f"Unknown child label in fixture edge: '{child_label}'.")
        session.add(
            ThemeHierarchyRelationship(
                id=uuid4(),
                codebook_id=codebook_id,
                parent_theme_id=theme_ids_by_label[parent_label],
                child_theme_id=theme_ids_by_label[child_label],
                is_active=True,
            )
        )

    await session.commit()

    occurrence_count_by_theme_id: dict[UUID, int] = {}
    for label, theme_id in theme_ids_by_label.items():
        occurrence_count_by_theme_id[theme_id] = (
            occurrence_count_by_label[label]
            if occurrence_count_by_label and label in occurrence_count_by_label
            else 1
        )

    return ThemeFixtureSeed(
        codebook_id=codebook_id,
        theme_ids_by_label=theme_ids_by_label,
        occurrence_count_by_theme_id=occurrence_count_by_theme_id,
    )


async def seed_unbalanced_dummy_tree(session: AsyncSession) -> DummyTreeIds:
    """Seed a codebook with two roots and an unbalanced theme tree."""
    seed = await _seed_codebook_with_theme_graph(
        session,
        project_id="project_theme_tree",
        name="Theme Tree Codebook",
        theme_labels=[
            "Product Delivery",
            "Team Coordination",
            "Release Predictability",
            "Incident Recovery",
            "Handover Quality",
            "Playbook Quality",
        ],
        edges_by_label=[
            ("Product Delivery", "Release Predictability"),
            ("Product Delivery", "Incident Recovery"),
            ("Incident Recovery", "Playbook Quality"),
            ("Team Coordination", "Handover Quality"),
        ],
    )
    return DummyTreeIds(
        codebook_id=seed.codebook_id,
        root_product=seed.theme_ids_by_label["Product Delivery"],
        root_team=seed.theme_ids_by_label["Team Coordination"],
        sub_release=seed.theme_ids_by_label["Release Predictability"],
        sub_incident=seed.theme_ids_by_label["Incident Recovery"],
        sub_handover=seed.theme_ids_by_label["Handover Quality"],
        leaf_playbook=seed.theme_ids_by_label["Playbook Quality"],
    )


async def seed_three_theme_codebook(session: AsyncSession) -> ThemeFixtureSeed:
    """DoD fixture: small codebook with 3 themes."""
    return await _seed_codebook_with_theme_graph(
        session,
        project_id="project_theme_tree_small",
        name="Theme Tree Small (3)",
        theme_labels=[
            "Delivery Confidence",
            "Planning Clarity",
            "Scope Stability",
        ],
        edges_by_label=[
            ("Delivery Confidence", "Planning Clarity"),
            ("Delivery Confidence", "Scope Stability"),
        ],
    )


async def seed_fifteen_theme_codebook(session: AsyncSession) -> ThemeFixtureSeed:
    """DoD fixture: larger codebook with 15 themes."""
    return await _seed_codebook_with_theme_graph(
        session,
        project_id="project_theme_tree_medium",
        name="Theme Tree Medium (15)",
        theme_labels=[
            "Strategy Alignment",
            "Execution Clarity",
            "Risk Ownership",
            "Cross-Team Dependencies",
            "Stakeholder Communication",
            "Escalation Paths",
            "Runbook Quality",
            "On-Call Sustainability",
            "Incident Triage",
            "Postmortem Discipline",
            "Knowledge Sharing",
            "Release Cadence",
            "Testing Discipline",
            "Monitoring Coverage",
            "Customer Feedback Loop",
        ],
        edges_by_label=[
            ("Strategy Alignment", "Execution Clarity"),
            ("Strategy Alignment", "Risk Ownership"),
            ("Strategy Alignment", "Cross-Team Dependencies"),
            ("Execution Clarity", "Release Cadence"),
            ("Execution Clarity", "Testing Discipline"),
            ("Risk Ownership", "Escalation Paths"),
            ("Risk Ownership", "Incident Triage"),
            ("Cross-Team Dependencies", "Stakeholder Communication"),
            ("Cross-Team Dependencies", "Knowledge Sharing"),
            ("Incident Triage", "Postmortem Discipline"),
            ("Incident Triage", "Runbook Quality"),
            ("Runbook Quality", "On-Call Sustainability"),
            ("Testing Discipline", "Monitoring Coverage"),
            ("Monitoring Coverage", "Customer Feedback Loop"),
        ],
    )


async def seed_zero_occurrence_theme_corpus(session: AsyncSession) -> ThemeFixtureSeed:
    """
    DoD fixture: corpus-like occurrence metadata where some themes have zero hits.

    Occurrence values are returned as fixture metadata because the current schema
    does not persist per-theme occurrence counts.
    """
    return await _seed_codebook_with_theme_graph(
        session,
        project_id="project_theme_tree_zero_occurrence",
        name="Theme Tree Zero Occurrence",
        theme_labels=[
            "Coordination Quality",
            "Handoff Accuracy",
            "Deployment Safety",
            "Incident Preparedness",
            "Feedback Responsiveness",
            "Documentation Depth",
        ],
        edges_by_label=[
            ("Coordination Quality", "Handoff Accuracy"),
            ("Coordination Quality", "Deployment Safety"),
            ("Coordination Quality", "Incident Preparedness"),
            ("Coordination Quality", "Feedback Responsiveness"),
            ("Coordination Quality", "Documentation Depth"),
        ],
        occurrence_count_by_label={
            "Coordination Quality": 8,
            "Handoff Accuracy": 0,
            "Deployment Safety": 3,
            "Incident Preparedness": 0,
            "Feedback Responsiveness": 2,
            "Documentation Depth": 0,
        },
    )


async def seed_long_theme_names_codebook(session: AsyncSession) -> ThemeFixtureSeed:
    """DoD fixture: very long labels to validate UI truncation/wrapping behavior."""
    return await _seed_codebook_with_theme_graph(
        session,
        project_id="project_theme_tree_long_names",
        name="Theme Tree Long Labels",
        theme_labels=[
            "Cross-Functional Collaboration Breakdowns During High-Urgency Release Cutovers Across Distributed Teams",
            "Communication Latency Between Product Owners, Engineering Leads, and Incident Command During Escalations",
            "Incomplete Operational Readiness Reviews Before Production-Impacting Changes Are Approved",
        ],
        edges_by_label=[
            (
                "Cross-Functional Collaboration Breakdowns During High-Urgency Release Cutovers Across Distributed Teams",
                "Communication Latency Between Product Owners, Engineering Leads, and Incident Command During Escalations",
            ),
            (
                "Cross-Functional Collaboration Breakdowns During High-Urgency Release Cutovers Across Distributed Teams",
                "Incomplete Operational Readiness Reviews Before Production-Impacting Changes Are Approved",
            ),
        ],
        occurrence_count_by_label={
            "Cross-Functional Collaboration Breakdowns During High-Urgency Release Cutovers Across Distributed Teams": 5,
            "Communication Latency Between Product Owners, Engineering Leads, and Incident Command During Escalations": 0,
            "Incomplete Operational Readiness Reviews Before Production-Impacting Changes Are Approved": 1,
        },
    )


# ============================================================================
# DEMONSTRATION ONLY (writes to main / potentially production database)
# ============================================================================
# This section is intentionally isolated from pytest fixtures.
# Remove this section later without breaking tests.


async def seed_unbalanced_dummy_tree_main_db_for_demo_only() -> DummyTreeIds:
    """
    FOR DEMONSTRATION PURPOSES ONLY.

    This writes to the app `DATABASE_URL` from settings (main DB). In many
    deployments that can be a production database, so never call this from
    pytest. Pytest should only use the fixture builders above with test-owned
    sessions.
    """
    database_url = _resolve_main_database_url_for_demo_only()
    engine = create_async_engine(database_url, echo=False, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    try:
        async with session_factory() as session:
            return await seed_unbalanced_dummy_tree(session)
    finally:
        await engine.dispose()


def _resolve_main_database_url_for_demo_only() -> str:
    """
    Resolve DATABASE_URL for demo execution without relying on current cwd.

    Priority:
    1) process environment variable DATABASE_URL
    2) Backend/.env file next to the app package
    """
    env_database_url = os.getenv("DATABASE_URL")
    if env_database_url:
        return env_database_url

    backend_root = Path(__file__).resolve().parents[2]
    env_path = backend_root / ".env"
    if env_path.exists():
        file_database_url = dotenv_values(env_path).get("DATABASE_URL")
        if isinstance(file_database_url, str) and file_database_url.strip():
            return file_database_url.strip()

    raise RuntimeError(
        "DEMO ONLY fixture runner requires DATABASE_URL. "
        "Set environment variable DATABASE_URL or define DATABASE_URL in "
        f"{env_path}."
    )


if __name__ == "__main__":
    ids = asyncio.run(seed_unbalanced_dummy_tree_main_db_for_demo_only())
    print(
        "DEMO ONLY: Seeded theme graph fixture into main DB "
        "(potentially production, depending on DATABASE_URL) "
        f"with codebook_id={ids.codebook_id}"
    )
