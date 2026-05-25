from uuid import UUID

from app.models.themes import NodeType
from app.schemas.common import BaseSchema


class ThemeSchema(BaseSchema):
    id: UUID
    node_type: NodeType
    label: str
    description: str | None = None
    is_active: bool
