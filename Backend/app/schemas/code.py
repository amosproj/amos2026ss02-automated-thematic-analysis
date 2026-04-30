from uuid import UUID

from app.domain.enums import ActorType, NodeStatus
from app.schemas.common import BaseSchema


class CodeSchema(BaseSchema):
    id: UUID
    label: str
    description: str
    status: NodeStatus
    created_by: ActorType
