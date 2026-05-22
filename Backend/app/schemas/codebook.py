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
    transcript_document_ids: list[UUID]

    @field_validator("codebook_name")
    @classmethod
    def normalize_codebook_name(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("codebook_name must not be empty")
        return normalized

    @field_validator("transcript_document_ids")
    @classmethod
    def validate_transcript_document_ids(cls, values: list[UUID]) -> list[UUID]:
        if not values:
            raise ValueError("transcript_document_ids must contain at least one document id")
        return values


class GeneratedCodebookResponse(BaseSchema):
    codebook: CodebookSchema
    transcripts_processed: int
    passages_processed: int
    themes_created: int
    codes_created: int
