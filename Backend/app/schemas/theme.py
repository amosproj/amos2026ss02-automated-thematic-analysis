import uuid

from app.domain.enums import ActorType, NodeStatus, ThemeLevel
from app.schemas.common import BaseSchema


class ThemeSchema(BaseSchema):
    id: uuid.UUID
    label: str
    description: str
    level: ThemeLevel
    status: NodeStatus
    created_by: ActorType
