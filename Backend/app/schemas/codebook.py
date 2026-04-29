from app.domain.enums import ActorType, CodebookStatus
from app.schemas.common import BaseSchema


class CodebookSchema(BaseSchema):
    id: str
    project_id: str
    previous_version_id: str | None = None
    name: str
    description: str | None = None
    research_question: str
    version: int
    status: CodebookStatus
    created_by: ActorType
