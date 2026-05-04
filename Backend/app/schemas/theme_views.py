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
