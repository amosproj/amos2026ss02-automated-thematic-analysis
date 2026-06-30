from __future__ import annotations

from uuid import UUID

from pydantic import Field

from app.schemas.common import BaseSchema
from app.schemas.theme_graph import ThemeTreeNode


class ThemeTreeResponse(BaseSchema):
    """Minimal wrapper payload for a codebook-scoped theme tree."""

    codebook_id: UUID
    root_theme_id: UUID | None = None
    tree: list[ThemeTreeNode]


class ThemeFrequencyItem(BaseSchema):
    """Flat frequency projection for one theme."""

    theme_id: UUID
    theme_name: str
    occurrence_count: int = Field(ge=0)
    interview_coverage_percentage: float = Field(ge=0.0, le=100.0)


class ThemeQuoteItem(BaseSchema):
    """One quote assigned to a theme, with its source document and interviewee."""

    quote: str
    confidence: float
    document_id: UUID
    document_title: str
    interviewee_id: str | None


class DemographicDimensionsResponse(BaseSchema):
    """Demographic variables available for breaking down a theme in one corpus."""

    dimensions: list[str]


class DemographicGroupStat(BaseSchema):
    """Theme frequency within one group of a demographic dimension."""

    group_value: str
    present_count: int = Field(ge=0)
    group_total: int = Field(ge=0)
    percentage: float = Field(ge=0.0, le=100.0)
    small_sample: bool = False


class ThemeDimensionBreakdown(BaseSchema):
    """Theme frequency broken down by the groups of one demographic dimension."""

    dimension: str
    groups: list[DemographicGroupStat]


class ThemeDemographicBreakdownResponse(BaseSchema):
    """Per-theme demographic breakdown across one or more selected dimensions."""

    theme_id: UUID
    application_run_id: UUID | None = None
    dimensions: list[ThemeDimensionBreakdown]
