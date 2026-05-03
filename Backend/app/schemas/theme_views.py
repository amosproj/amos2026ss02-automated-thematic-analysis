from __future__ import annotations

from uuid import UUID

from app.schemas.common import BaseSchema
from app.schemas.theme_graph import ThemeTreeNode


class ThemeTreeResponse(BaseSchema):
    """Minimal wrapper payload for a codebook-scoped theme tree."""

    codebook_id: UUID
    root_theme_id: UUID | None = None
    tree: list[ThemeTreeNode]
