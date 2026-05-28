from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator

from app.schemas.common import BaseSchema


class CodebookSchema(BaseSchema):
    """TODO: Unfinished placeholder schema."""

    id: UUID
    project_id: str
    name: str
    description: str | None = None
    version: int
    created_by: str


class CodebookGenerateRequest(BaseSchema):
    codebook_name: str = Field(min_length=1, max_length=255)
    corpus_id: UUID
    transcript_document_ids: list[UUID] | None = None

    @field_validator("codebook_name")
    @classmethod
    def normalize_codebook_name(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("codebook_name must not be empty")
        return normalized

    @field_validator("transcript_document_ids")
    @classmethod
    def normalize_transcript_document_ids(cls, values: list[UUID] | None) -> list[UUID] | None:
        if values is None:
            return None
        if not values:
            return None
        return values


class GeneratedCodebookResponse(BaseSchema):
    class PassageFailure(BaseSchema):
        passage_index: int
        passage_excerpt: str
        error: str
        attempts: int

    codebook: CodebookSchema
    transcripts_processed: int
    passages_processed: int
    themes_created: int
    codes_created: int
    passages_failed: int = 0
    failed_passages: list[PassageFailure] = Field(default_factory=list)


JobStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]


class CodebookGenerationJobCreateRequest(CodebookGenerateRequest):
    pass


class CodebookGenerationJobSchema(BaseSchema):
    id: UUID
    status: JobStatus
    phase: str
    progress_percent: int
    codebook_name: str
    corpus_id: UUID
    transcript_document_ids: list[UUID]
    cancel_requested: bool

    codebook_id: UUID | None = None
    passages_total: int
    passages_done: int
    transcripts_processed: int | None = None
    passages_processed: int | None = None
    themes_created: int | None = None
    codes_created: int | None = None
    error_message: str | None = None

    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
