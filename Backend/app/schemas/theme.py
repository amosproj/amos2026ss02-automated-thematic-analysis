from uuid import UUID

from app.schemas.common import BaseSchema


class ThemeSchema(BaseSchema):
    id: UUID
    label: str
    description: str | None = None
    is_active: bool
