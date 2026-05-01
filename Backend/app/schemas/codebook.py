from uuid import UUID

from app.schemas.common import BaseSchema


class CodebookSchema(BaseSchema):
    """TODO: Unfinished placeholder schema."""

    id: UUID
    project_id: str
    name: str
    description: str | None = None
    version: int
    created_by: str
