from __future__ import annotations

"""Read-model schemas for theme tree and codebook theme summaries."""

from uuid import UUID

from app.domain.enums import CodebookStatus, ThemeLevel
from app.schemas.common import BaseSchema
from app.schemas.theme_graph import ThemeTreeNode


class ResolvedCodebookContext(BaseSchema):
    """Resolved version context used by version-aware endpoints."""

    project_id: str
    codebook_id: UUID
    codebook_version: int
    codebook_name: str
    codebook_status: CodebookStatus


class ThemeTreeResponse(BaseSchema):
    """Payload for a resolved codebook theme tree."""

    codebook: ResolvedCodebookContext
    root_theme_id: UUID | None = None
    include_candidate_nodes: bool
    tree: list[ThemeTreeNode]


class ThemeFrequencyItem(BaseSchema):
    """Frequency view for one theme in a resolved codebook."""

    theme_id: UUID
    theme_name: str
    theme_level: ThemeLevel
    occurrence_count: int
    interview_coverage_percentage: float


class ThemeFrequencyResponse(BaseSchema):
    """List payload for all themes in one resolved codebook version."""

    codebook: ResolvedCodebookContext
    total_interviews_in_corpus: int
    themes: list[ThemeFrequencyItem]
