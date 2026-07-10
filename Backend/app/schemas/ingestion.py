import uuid
from datetime import datetime
from typing import Any

from app.schemas.common import BaseSchema


class CorpusCreate(BaseSchema):
    """Request body for creating a new corpus."""

    corpus_id: uuid.UUID
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


class CopyDocumentsRequest(BaseSchema):
    """Request body for copying documents to another corpus."""

    target_corpus_id: uuid.UUID
    document_ids: list[uuid.UUID]


class CorpusDocumentSchema(BaseSchema):
    """API response shape for a stored document (no content — fetch via document endpoint)."""

    id: uuid.UUID
    corpus_id: uuid.UUID
    title: str
    filename: str | None = None
    created_at: datetime
    updated_at: datetime


class CorpusDocumentContentSchema(CorpusDocumentSchema):
    """API response shape for a single document including its full text content."""

    content: str
    demographic_data: dict[str, Any] | None = None


class IngestResultSchema(BaseSchema):
    """Summary returned after an ingestion call completes."""

    documents_created: int


class UploadFileResult(BaseSchema):
    """Per-file result for a multi-file upload."""

    filename: str            # original (client-provided) filename
    stored_filename: str | None = None  # filename actually stored (may differ if renamed)
    success: bool
    documents_created: int = 0
    error: str | None = None


class MultiUploadResultSchema(BaseSchema):
    """Aggregate response for the multi-file upload endpoint."""

    results: list[UploadFileResult]
