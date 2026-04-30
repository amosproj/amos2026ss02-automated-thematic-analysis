from __future__ import annotations

"""Read-side service for version-aware theme tree and theme summary endpoints."""

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import CodebookStatus, CodebookThemeRelationshipType, NodeStatus, RelationshipStatus
from app.models import Codebook, CodebookThemeRelationship, Theme
from app.schemas.theme_views import (
    ResolvedCodebookContext,
    ThemeFrequencyItem,
    ThemeFrequencyResponse,
    ThemeTreeResponse,
)
from app.services.theme_graph import DEFAULT_WORKING_THEME_STATUSES, ThemeGraphService


class CodebookResolutionError(Exception):
    """Raised when a codebook cannot be resolved for a project/version selector."""


@dataclass(slots=True, frozen=True)
class ResolvedCodebook:
    """Internal resolved codebook row used by read methods."""

    id: UUID
    project_id: str
    name: str
    version: int
    status: CodebookStatus

    def to_context(self) -> ResolvedCodebookContext:
        return ResolvedCodebookContext(
            project_id=self.project_id,
            codebook_id=self.id,
            codebook_version=self.version,
            codebook_name=self.name,
            codebook_status=self.status,
        )


class ThemeReadService:
    """
    Read-only theme service.

    All public methods are version-aware:
    - if `version` is provided, resolve that codebook version for the project.
    - if omitted, resolve the most recent codebook by descending version.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_theme_tree_for_project(
        self,
        *,
        project_id: str,
        version: int | None = None,
        root_theme_id: UUID | None = None,
        include_candidate_nodes: bool = True,
    ) -> ThemeTreeResponse:
        resolved = await self.resolve_codebook(project_id=project_id, version=version)
        graph_service = ThemeGraphService(self._session, auto_commit=False)
        tree = await graph_service.auto_generate_theme_tree_for_codebook(
            codebook_id=resolved.id,
            root_theme_id=root_theme_id,
            include_candidate_nodes=include_candidate_nodes,
        )
        return ThemeTreeResponse(
            codebook=resolved.to_context(),
            root_theme_id=root_theme_id,
            include_candidate_nodes=include_candidate_nodes,
            tree=tree,
        )

    async def get_theme_frequency_for_project(
        self,
        *,
        project_id: str,
        version: int | None = None,
        include_candidate_nodes: bool = True,
    ) -> ThemeFrequencyResponse:
        resolved = await self.resolve_codebook(project_id=project_id, version=version)
        statuses = (
            set(DEFAULT_WORKING_THEME_STATUSES)
            if include_candidate_nodes
            else {NodeStatus.ACTIVE}
        )
        themes = await self._load_themes_for_codebook(codebook_id=resolved.id, statuses=statuses)

        items = [
            ThemeFrequencyItem(
                theme_id=theme.id,
                theme_name=theme.label,
                theme_level=theme.level,
                occurrence_count=0,  # TODO: replace with real occurrence counts.
                interview_coverage_percentage=0.0,  # TODO: compute % of interviews.
            )
            for theme in themes
        ]
        items.sort(key=lambda item: (-item.occurrence_count, item.theme_name.lower(), item.theme_id))

        return ThemeFrequencyResponse(
            codebook=resolved.to_context(),
            total_interviews_in_corpus=0,  # TODO: replace with real corpus interview count.
            themes=items,
        )

    async def resolve_codebook(
        self, *, project_id: str, version: int | None = None
    ) -> ResolvedCodebook:
        """Resolve one codebook by project and optional version selector."""
        stmt = select(Codebook).where(Codebook.project_id == project_id)
        if version is not None:
            stmt = stmt.where(Codebook.version == version)
        else:
            stmt = stmt.order_by(desc(Codebook.version))
        codebook = (await self._session.execute(stmt.limit(1))).scalar_one_or_none()
        if codebook is None:
            selector = f"version={version}" if version is not None else "latest version"
            raise CodebookResolutionError(
                f"Could not resolve codebook for project '{project_id}' using {selector}."
            )
        return ResolvedCodebook(
            id=codebook.id,
            project_id=codebook.project_id,
            name=codebook.name,
            version=codebook.version,
            status=codebook.status,
        )

    async def _load_themes_for_codebook(
        self, *, codebook_id: UUID, statuses: set[NodeStatus]
    ) -> list[Theme]:
        if not statuses:
            return []
        stmt = (
            select(Theme)
            .join(
                CodebookThemeRelationship,
                and_(
                    CodebookThemeRelationship.theme_id == Theme.id,
                    CodebookThemeRelationship.codebook_id == codebook_id,
                    CodebookThemeRelationship.relationship_type
                    == CodebookThemeRelationshipType.CONTAINS,
                    CodebookThemeRelationship.status == RelationshipStatus.ACTIVE,
                ),
            )
            .where(Theme.status.in_(statuses))
        )
        return list((await self._session.scalars(stmt)).all())
