import math
import uuid

from fastapi import APIRouter, UploadFile

from app.dependencies import AppSettings, DbSession
from app.exceptions import UnprocessableError
from app.schemas.common import Page, PageMeta, ResponseEnvelope
from app.schemas.ingestion import (
    BulkDocumentIngestRequest,
    CorpusChunkSchema,
    CorpusCreate,
    CorpusDocumentSchema,
    CorpusSchema,
    IngestResultSchema,
)
from app.services.ingestion import (
    IngestResult,
    IngestionService,
    parse_jsonl_upload,
)

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

_SUPPORTED_EXTENSIONS = {".jsonl"}


def _pages(total: int, page_size: int) -> int:
    """Calculate total number of pages for pagination metadata."""
    return math.ceil(total / page_size) if total > 0 else 0


def _to_result_schema(result: IngestResult) -> IngestResultSchema:
    """Convert the internal IngestResult dataclass to the API response schema."""
    return IngestResultSchema(
        documents_created=len(result.documents),
        chunks_created=result.chunks_created,
    )


# ---------------------------------------------------------------------------
# Corpus endpoints
# ---------------------------------------------------------------------------


@router.post("/corpora", response_model=ResponseEnvelope[CorpusSchema], status_code=201)
async def create_corpus(
    payload: CorpusCreate,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[CorpusSchema]:
    service = IngestionService(session, settings)
    corpus = await service.create_corpus(payload)
    return ResponseEnvelope.ok(CorpusSchema.model_validate(corpus))


@router.get("/corpora", response_model=ResponseEnvelope[Page[CorpusSchema]])
async def list_corpora(
    session: DbSession,
    settings: AppSettings,
    project_id: uuid.UUID | None = None,  # TODO: Only placeholder for now. add Project Data Structure and wire correctly into Corpus. optional filter
    page: int = 1,
    page_size: int = 20,
) -> ResponseEnvelope[Page[CorpusSchema]]:
    service = IngestionService(session, settings)
    corpora, total = await service.list_corpora(project_id=project_id, page=page, page_size=page_size)
    return ResponseEnvelope.ok(
        Page(
            items=[CorpusSchema.model_validate(c) for c in corpora],
            meta=PageMeta(total=total, page=page, page_size=page_size, pages=_pages(total, page_size)),
        )
    )


@router.get("/corpora/{corpus_id}", response_model=ResponseEnvelope[CorpusSchema])
async def get_corpus(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[CorpusSchema]:
    service = IngestionService(session, settings)
    corpus = await service.get_corpus(corpus_id)
    return ResponseEnvelope.ok(CorpusSchema.model_validate(corpus))


# ---------------------------------------------------------------------------
# Document ingestion endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/corpora/{corpus_id}/documents/bulk",
    response_model=ResponseEnvelope[IngestResultSchema],
    status_code=201,
)
async def bulk_ingest_documents(
    corpus_id: uuid.UUID,
    payload: BulkDocumentIngestRequest,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[IngestResultSchema]:
    """Ingest a list of documents provided directly in the request body."""
    service = IngestionService(session, settings)
    result = await service.ingest_documents(
        corpus_id=corpus_id,
        documents=payload.documents,
    )
    return ResponseEnvelope.ok(_to_result_schema(result))


@router.post(
    "/corpora/{corpus_id}/upload",
    response_model=ResponseEnvelope[IngestResultSchema],
    status_code=201,
)
async def upload_documents(
    corpus_id: uuid.UUID,
    file: UploadFile,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[IngestResultSchema]:
    """Accept a file upload (.txt / .json / .jsonl / .csv) and ingest its contents."""
    filename = file.filename or ""
    dot_pos = filename.rfind(".")
    ext = filename[dot_pos:].lower() if dot_pos != -1 else ""

    if ext not in _SUPPORTED_EXTENSIONS:
        raise UnprocessableError(
            f"Unsupported file extension '{ext}'. Supported: {', '.join(sorted(_SUPPORTED_EXTENSIONS))}"
        )

    content = await file.read()
    docs = parse_jsonl_upload(filename, content)

    service = IngestionService(session, settings)
    result = await service.ingest_documents(
        corpus_id=corpus_id,
        documents=docs,
        filename=filename,
    )
    return ResponseEnvelope.ok(_to_result_schema(result))


# ---------------------------------------------------------------------------
# Read-back endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/corpora/{corpus_id}/documents",
    response_model=ResponseEnvelope[Page[CorpusDocumentSchema]],
)
async def list_documents(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
    page: int = 1,
    page_size: int = 20,
) -> ResponseEnvelope[Page[CorpusDocumentSchema]]:
    service = IngestionService(session, settings)
    documents, total = await service.list_documents(corpus_id=corpus_id, page=page, page_size=page_size)
    return ResponseEnvelope.ok(
        Page(
            items=[CorpusDocumentSchema.model_validate(d) for d in documents],
            meta=PageMeta(total=total, page=page, page_size=page_size, pages=_pages(total, page_size)),
        )
    )


@router.get(
    "/corpora/{corpus_id}/chunks",
    response_model=ResponseEnvelope[Page[CorpusChunkSchema]],
)
async def list_chunks(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
    document_id: uuid.UUID | None = None,  # optional filter to a single document
    page: int = 1,
    page_size: int = 20,
) -> ResponseEnvelope[Page[CorpusChunkSchema]]:
    service = IngestionService(session, settings)
    chunks, total = await service.list_chunks(
        corpus_id=corpus_id, document_id=document_id, page=page, page_size=page_size
    )
    return ResponseEnvelope.ok(
        Page(
            items=[CorpusChunkSchema.model_validate(c) for c in chunks],
            meta=PageMeta(total=total, page=page, page_size=page_size, pages=_pages(total, page_size)),
        )
    )
