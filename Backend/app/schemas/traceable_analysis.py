from __future__ import annotations

from uuid import UUID

from app.schemas.common import BaseSchema


class TraceableAnalysisResult(BaseSchema):
    codebook_id: UUID
    application_run_id: UUID | None = None
    documents_processed: int
    analysis_units_processed: int
    quotes_created: int
    codes_created: int
    themes_created: int
    documents_coded: int
    documents_failed: int
    provenance: dict[str, object]
    action_log: list[dict[str, object]]
