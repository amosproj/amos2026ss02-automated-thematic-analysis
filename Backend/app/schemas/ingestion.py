import uuid
from datetime import datetime

from app.schemas.common import BaseSchema


class CorpusCreate(BaseSchema):
    """Request body for creating a new corpus."""

    project_id: uuid.UUID
    name: str


class CorpusSchema(BaseSchema):
    """API response shape for a corpus."""

    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    created_at: datetime
    updated_at: datetime


class DocumentInput(BaseSchema):
    """One document to be ingested. Used both as bulk-request body and as parser output."""

    title: str | None = None  # falls back to filename or "Untitled" if not provided
    text: str


class BulkDocumentIngestRequest(BaseSchema):
    """Request body for the bulk ingestion endpoint."""

    documents: list[DocumentInput]


class CorpusDocumentSchema(BaseSchema):
    """API response shape for a stored document (no text — fetch chunks for content)."""

    id: uuid.UUID
    corpus_id: uuid.UUID
    title: str
    created_at: datetime
    updated_at: datetime


class CorpusChunkSchema(BaseSchema):
    """API response shape for a single chunk."""

    id: uuid.UUID
    document_id: uuid.UUID
    chunk_index: int
    text: str
    created_at: datetime
    updated_at: datetime


class IngestResultSchema(BaseSchema):
    """Summary returned after an ingestion call completes."""

    documents_created: int
    chunks_created: int
