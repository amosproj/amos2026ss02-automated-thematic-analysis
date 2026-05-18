import datetime
import uuid
from typing import Any

from pydantic import Field

from app.schemas import BaseSchema

class ImportDemographicPreview(BaseSchema):
    rows_detected: int
    columns_detected: int
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)


class ImportDemographicResponse(BaseSchema):
    import_id: uuid.UUID
    name: str
    status: str
    preview: ImportDemographicPreview
    expires_at: datetime.datetime


class UploadDemographicConfirmResponse(BaseSchema):
    """Summary returned after a demographic upload confirmation."""
    import_id: uuid.UUID
    name: str
    rows_created: int = 0
    status: str


class DemographicFileSummary(BaseSchema):
    id: uuid.UUID
    corpus_id: uuid.UUID
    name: str
    original_columns: list[str]
    rows_total: int
    created_at: datetime.datetime
    updated_at: datetime.datetime


class DemographicRowSchema(BaseSchema):
    id: uuid.UUID
    demographic_file_id: uuid.UUID
    interviewee_id: str
    row_number: int
    data: dict[str, Any]
