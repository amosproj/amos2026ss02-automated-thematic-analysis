from __future__ import annotations

from uuid import UUID

from pydantic import Field

from app.schemas.common import BaseSchema


class ThemeNodeView(BaseSchema):
    """Minimal serializable projection of one theme node."""

    id: UUID
    label: str
    is_active: bool


class ThemeEdgeView(BaseSchema):
    """One hierarchy edge (child -> parent) inside a codebook."""

    child_theme_id: UUID
    parent_theme_id: UUID


class ThemeDagValidation(BaseSchema):
    """Validation report for hierarchy constraints."""

    is_valid: bool
    violations: tuple[str, ...] = ()


class ThemeDagView(BaseSchema):
    """DAG-like projection over hierarchy edges."""

    codebook_id: UUID
    nodes: dict[UUID, ThemeNodeView]
    edges: list[ThemeEdgeView]
    root_theme_ids: list[UUID]


class ThemeTreeNode(BaseSchema):
    """Recursive tree view used by API/read service."""

    theme: ThemeNodeView
    children: list["ThemeTreeNode"] = Field(default_factory=list)


# Resolve the forward reference for recursive children typing.
ThemeTreeNode.model_rebuild()
