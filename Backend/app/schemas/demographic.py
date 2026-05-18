import datetime
import uuid
from typing import Any

from pydantic import Field

from app.schemas import BaseSchema

class ImportDemographicPreview(BaseSchema): # TODO
    rows_detected: int
    #valid_rows: int | None
    #invalid_rows: int | None
    columns_detected: int
    #valid_columns: int | None
    #invalid_columns: int | None
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)


class ImportDemographicResponse(BaseSchema):
    import_id: uuid.UUID
    status: str
    preview: ImportDemographicPreview
    expires_at: datetime.datetime


class UploadDemographicConfirmResponse(BaseSchema):
    """Summary returned after a demograhic upload confirmation."""
    import_id: uuid.UUID
    rows_created: int = 0
    status: str
