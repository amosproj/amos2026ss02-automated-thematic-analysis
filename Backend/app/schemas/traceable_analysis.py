from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator

from app.schemas.common import BaseSchema
from app.utils.sanitize import sanitize_research_query

JobStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]

_QUERY_MIN = 10
_QUERY_MAX = 500


class TraceableAnalysisJobCreateRequest(BaseSchema):
    """Create a one-step quote-grounded codebook generation and application job."""

    codebook_name: str = Field(min_length=1, max_length=255)
    analysis_name: str | None = Field(default=None, max_length=255)
    custom_id: str | None = Field(default=None, max_length=255)
    corpus_id: UUID
    transcript_document_ids: list[UUID] | None = Field(
        default=None,
        description="Selected transcript IDs. Omit or pass an empty list to use all corpus transcripts.",
    )
    research_query: str | None = Field(
        default=None,
        max_length=_QUERY_MAX,
        description="Optional research question guiding quote-code extraction.",
    )
    researcher_topics: str | None = Field(
        default=None,
        max_length=_QUERY_MAX,
        description="Optional comma-separated topics the researcher wants the analysis to cover.",
    )
    max_refinement_rounds: int = Field(
        default=1,
        ge=0,
        le=3,
        description="Maximum conservative reviewer/refinement rounds after theme synthesis.",
    )

    @field_validator("codebook_name")
    @classmethod
    def normalize_codebook_name(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("codebook_name must not be empty")
        return normalized

    @field_validator("analysis_name", "custom_id")
    @classmethod
    def normalize_optional_label(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        return normalized or None

    @field_validator("transcript_document_ids")
    @classmethod
    def normalize_transcript_document_ids(cls, values: list[UUID] | None) -> list[UUID] | None:
        if not values:
            return None
        ordered_unique: list[UUID] = []
        seen: set[UUID] = set()
        for document_id in values:
            if document_id in seen:
                continue
            seen.add(document_id)
            ordered_unique.append(document_id)
        return ordered_unique

    @field_validator("research_query", mode="before")
    @classmethod
    def sanitize_and_validate_query(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            return value  # type: ignore[return-value]
        if not value.strip():
            raise ValueError("research_query must not be empty or whitespace only.")
        cleaned = sanitize_research_query(value)
        if len(cleaned) < _QUERY_MIN:
            raise ValueError(
                f"research_query must be at least {_QUERY_MIN} characters (got {len(cleaned)})."
            )
        if len(cleaned) > _QUERY_MAX:
            raise ValueError(
                f"research_query must be at most {_QUERY_MAX} characters (got {len(cleaned)})."
            )
        return cleaned

    @field_validator("researcher_topics")
    @classmethod
    def sanitize_and_validate_topics(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = sanitize_research_query(value)
        if not cleaned:
            return None
        if len(cleaned) > _QUERY_MAX:
            raise ValueError(
                f"researcher_topics must be at most {_QUERY_MAX} characters (got {len(cleaned)})."
            )
        return cleaned


class TraceableAnalysisJobSchema(BaseSchema):
    id: UUID
    status: JobStatus
    phase: str
    progress_percent: int
    codebook_name: str
    analysis_name: str | None = None
    custom_id: str | None = None
    corpus_id: UUID
    transcript_document_ids: list[UUID]
    cancel_requested: bool
    codebook_id: UUID | None = None
    application_run_id: UUID | None = None
    documents_total: int
    documents_done: int
    analysis_units_total: int
    analysis_units_done: int
    quotes_created: int | None = None
    codes_created: int | None = None
    themes_created: int | None = None
    documents_coded: int | None = None
    documents_failed: int | None = None
    research_query: str | None = None
    researcher_topics: str | None = None
    max_refinement_rounds: int
    error_message: str | None = None
    provenance_json: str | None = None
    action_log_json: str | None = None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class TraceableAnalysisResult(BaseSchema):
    codebook_id: UUID
    application_run_id: UUID
    documents_processed: int
    analysis_units_processed: int
    quotes_created: int
    codes_created: int
    themes_created: int
    documents_coded: int
    documents_failed: int
    provenance: dict[str, object]
    action_log: list[dict[str, object]]
