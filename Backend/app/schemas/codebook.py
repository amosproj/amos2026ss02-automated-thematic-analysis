import uuid

from app.domain.enums import ActorType, CodebookStatus
from app.schemas.common import BaseSchema


class CodebookSchema(BaseSchema):
    id: uuid.UUID
    project_id: uuid.UUID
    previous_version_id: uuid.UUID | None = None
    name: str
    description: str | None = None
    research_question: str
    version: int
    status: CodebookStatus
    created_by: ActorType
