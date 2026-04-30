import uuid
from datetime import datetime
from typing import Any

from pydantic import Field

from app.domain.enums import DocumentStatus, IngestionRunStatus, SourceType
from app.schemas.common import BaseSchema


class CorpusCreate(BaseSchema):
    project_id: uuid.UUID
    name: str
    description: str | None = None
    research_question: str | None = None
    metadata: dict[str, Any] = {}


class CorpusSchema(BaseSchema):
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    description: str | None = None
    research_question: str | None = None
    # ORM attribute is extra_metadata; API field is metadata
    metadata: dict[str, Any] = Field(validation_alias="extra_metadata", default_factory=dict)
    created_at: datetime
    updated_at: datetime


class DocumentInput(BaseSchema):
    external_id: str | None = None
    title: str | None = None
    text: str
    metadata: dict[str, Any] = {}


class BulkDocumentIngestRequest(BaseSchema):
    documents: list[DocumentInput]
    source_type: SourceType = SourceType.MANUAL


class CorpusDocumentSchema(BaseSchema):
    id: uuid.UUID
    corpus_id: uuid.UUID
    external_id: str | None = None
    title: str | None = None
    text_hash: str
    word_count: int
    source_type: SourceType
    status: DocumentStatus
    metadata: dict[str, Any] = Field(validation_alias="extra_metadata", default_factory=dict)
    created_at: datetime
    updated_at: datetime


class CorpusChunkSchema(BaseSchema):
    id: uuid.UUID
    document_id: uuid.UUID
    chunk_index: int
    start_word: int
    end_word: int
    text: str
    text_hash: str
    word_count: int
    created_at: datetime
    updated_at: datetime


class IngestionRunSchema(BaseSchema):
    id: uuid.UUID
    corpus_id: uuid.UUID
    source_type: SourceType
    status: IngestionRunStatus
    filename: str | None = None
    total_documents: int
    accepted_documents: int
    rejected_documents: int
    duplicate_documents: int
    empty_documents: int
    parameters: dict[str, Any]
    error_message: str | None = None
    created_at: datetime
    updated_at: datetime


class IngestionResultSchema(BaseSchema):
    run: IngestionRunSchema
    documents: list[CorpusDocumentSchema]
    chunks_created: int
