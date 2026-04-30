from uuid import UUID

from app.domain.enums import ActorType, CodebookStatus
from app.schemas.common import BaseSchema


class CodebookSchema(BaseSchema):
    id: UUID
    project_id: str
    previous_version_id: UUID | None = None
    name: str
    description: str | None = None
    version: int
    status: CodebookStatus
    created_by: ActorType
