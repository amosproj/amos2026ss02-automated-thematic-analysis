from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator

from app.schemas.common import BaseSchema

JobStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]
RunStatus = Literal["running", "succeeded", "failed", "cancelled"]


class CodebookApplicationJobCreateRequest(BaseSchema):
    corpus_id: UUID
    transcript_document_ids: list[UUID] | None = Field(
        default=None,
        description="Selected transcript IDs. Omit or pass an empty list to apply to all corpus transcripts.",
    )

    @field_validator("transcript_document_ids")
    @classmethod
    def normalize_transcript_document_ids(cls, values: list[UUID] | None) -> list[UUID] | None:
        if values is None or not values:
            return None
        ordered_unique: list[UUID] = []
        seen: set[UUID] = set()
        for document_id in values:
            if document_id in seen:
                continue
            seen.add(document_id)
            ordered_unique.append(document_id)
        return ordered_unique


class CodebookApplicationJobSchema(BaseSchema):
    id: UUID
    status: JobStatus
    phase: str
    progress_percent: int
    corpus_id: UUID
    codebook_id: UUID
    transcript_document_ids: list[UUID]
    cancel_requested: bool
    application_run_id: UUID | None = None
    documents_total: int
    documents_done: int
    documents_coded: int | None = None
    documents_failed: int | None = None
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class CodebookApplicationRunSchema(BaseSchema):
    id: UUID
    corpus_id: UUID
    codebook_id: UUID
    status: RunStatus
    documents_total: int
    documents_coded: int
    documents_failed: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class ThemeAssignmentSchema(BaseSchema):
    id: UUID
    theme_id: UUID
    is_present: bool
    confidence: float
    quote: str | None = None
    start_char: int | None = None
    end_char: int | None = None
    quote_match_status: str | None = None


class CodeAssignmentSchema(BaseSchema):
    id: UUID
    code_id: UUID
    theme_id: UUID | None = None
    quote: str
    start_char: int | None = None
    end_char: int | None = None
    quote_match_status: str
    confidence: float
    rationale: str | None = None


class DocumentCodingSchema(BaseSchema):
    id: UUID
    application_run_id: UUID
    document_id: UUID
    codebook_id: UUID
    status: str
    summary: str | None = None
    researcher_notes: str | None = None
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime
    theme_assignments: list[ThemeAssignmentSchema] = Field(default_factory=list)
    code_assignments: list[CodeAssignmentSchema] = Field(default_factory=list)


class CodebookApplicationRunDetailSchema(CodebookApplicationRunSchema):
    document_codings: list[DocumentCodingSchema] = Field(default_factory=list)

