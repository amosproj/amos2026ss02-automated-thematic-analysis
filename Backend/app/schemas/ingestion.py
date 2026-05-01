import uuid
from datetime import datetime

from app.schemas.common import BaseSchema


class CorpusCreate(BaseSchema):
    project_id: uuid.UUID
    name: str


class CorpusSchema(BaseSchema):
    id: uuid.UUID
    project_id: uuid.UUID
    name: str
    created_at: datetime
    updated_at: datetime


class DocumentInput(BaseSchema):
    title: str | None = None
    text: str


class BulkDocumentIngestRequest(BaseSchema):
    documents: list[DocumentInput]


class CorpusDocumentSchema(BaseSchema):
    id: uuid.UUID
    corpus_id: uuid.UUID
    title: str
    created_at: datetime
    updated_at: datetime


class CorpusChunkSchema(BaseSchema):
    id: uuid.UUID
    document_id: uuid.UUID
    chunk_index: int
    text: str
    created_at: datetime
    updated_at: datetime


class IngestResultSchema(BaseSchema):
    documents_created: int
    chunks_created: int
