from __future__ import annotations

"""Theme graph view schemas used as read models from the service layer."""

from pydantic import Field

from app.domain.enums import ActorType, NodeStatus, RelationshipStatus, ThemeLevel, ThemeRelationshipType
from app.schemas.common import BaseSchema


class ThemeNodeView(BaseSchema):
    """Serializable projection of one theme node in a codebook-scoped graph."""

    id: str
    label: str
    description: str
    level: ThemeLevel
    status: NodeStatus
    created_by: ActorType


class ThemeEdgeView(BaseSchema):
    """Serializable projection of one active edge between two theme nodes."""

    source_theme_id: str
    target_theme_id: str
    relationship_type: ThemeRelationshipType
    status: RelationshipStatus


class ThemeDagValidation(BaseSchema):
    """Validation report for hierarchy constraints over active `CHILD_OF` edges."""

    is_valid: bool
    violations: tuple[str, ...] = ()


class ThemeDagView(BaseSchema):
    """DAG projection used by callers that need adjacency access."""

    codebook_id: str
    nodes: dict[str, ThemeNodeView]
    adjacency: dict[str, list[ThemeEdgeView]]
    root_theme_ids: list[str]


class ThemeTreeNode(BaseSchema):
    """Recursive tree projection for UI and debugging of hierarchy structure."""

    theme: ThemeNodeView
    children: list["ThemeTreeNode"] = Field(default_factory=list)


# Resolve the forward reference for the recursive `children` field.
ThemeTreeNode.model_rebuild()
