from uuid import UUID

from app.schemas.common import BaseSchema


class ThemeSchema(BaseSchema):
    id: UUID
    label: str
    is_active: bool
