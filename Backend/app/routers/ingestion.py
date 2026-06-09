import math
import uuid

from fastapi import APIRouter, UploadFile

from app.dependencies import AppSettings, DbSession
from app.exceptions import UnprocessableError
from app.schemas.common import Page, PageMeta, ResponseEnvelope
from app.schemas.ingestion import (
    BulkDocumentIngestRequest,
    CorpusCreate,
    CorpusDocumentContentSchema,
    CorpusDocumentSchema,
    CorpusSchema,
    IngestResultSchema,
    MultiUploadResultSchema,
    UploadFileResult,
)
from app.services.ingestion import (
    IngestionService,
    IngestResult,
)
from app.services.upload_parsers import (
    SUPPORTED_EXTENSIONS,
    get_extension,
    parse_upload,
)

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


def _pages(total: int, page_size: int) -> int:
    """Calculate total number of pages for pagination metadata."""
    return math.ceil(total / page_size) if total > 0 else 0


def _to_result_schema(result: IngestResult) -> IngestResultSchema:
    """Convert the internal IngestResult dataclass to the API response schema."""
    return IngestResultSchema(
        documents_created=len(result.documents),
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
    service = IngestionService(session)
    corpus = await service.create_corpus(payload)
    return ResponseEnvelope.ok(CorpusSchema.model_validate(corpus))


@router.get("/corpora", response_model=ResponseEnvelope[Page[CorpusSchema]])
async def list_corpora(
    session: DbSession,
    settings: AppSettings,
    corpus_id: uuid.UUID | None = None,
    page: int = 1,
    page_size: int = 20,
) -> ResponseEnvelope[Page[CorpusSchema]]:
    service = IngestionService(session)
    corpora, total = await service.list_corpora(corpus_id=corpus_id, page=page, page_size=page_size)
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
    service = IngestionService(session)
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
    service = IngestionService(session)
    result = await service.ingest_documents(
        corpus_id=corpus_id,
        documents=payload.documents,
    )
    return ResponseEnvelope.ok(_to_result_schema(result))


async def _process_one_upload(
    service: IngestionService,
    corpus_id: uuid.UUID,
    file: UploadFile,
    max_bytes: int,
) -> UploadFileResult:
    """Parse and ingest a single uploaded file. Returns a per-file result; never
    raises (errors are captured as `success=False`)."""
    filename = file.filename or ""
    try:
        ext = get_extension(filename)
        if ext not in SUPPORTED_EXTENSIONS:
            raise UnprocessableError(
                f"Unsupported file extension '{ext}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
            )

        content = await file.read()
        if len(content) == 0:
            raise UnprocessableError(f"'{filename}': file is empty")
        if len(content) > max_bytes:
            raise UnprocessableError(
                f"'{filename}': file exceeds maximum size of {max_bytes} bytes"
            )

        docs = parse_upload(filename, content)
        result = await service.ingest_documents(
            corpus_id=corpus_id,
            documents=docs,
            filename=filename,
        )
        stored = result.documents[0].filename if result.documents else filename
        return UploadFileResult(
            filename=filename,
            stored_filename=stored,
            success=True,
            documents_created=len(result.documents),
        )
    except UnprocessableError as exc:
        return UploadFileResult(filename=filename, success=False, error=str(exc))


@router.post(
    "/corpora/{corpus_id}/upload",
    response_model=ResponseEnvelope[MultiUploadResultSchema],
    status_code=201,
)
async def upload_documents(
    corpus_id: uuid.UUID,
    files: list[UploadFile],
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[MultiUploadResultSchema]:
    """Accept one or more uploaded transcripts (.txt / .docx / .pdf / .jsonl) and
    ingest their contents. Each file produces an independent result, so a single
    bad file does not block the others."""
    service = IngestionService(session)
    results = [
        await _process_one_upload(
            service,
            corpus_id,
            f,
            max_bytes=settings.MAX_UPLOAD_BYTES,
        )
        for f in files
    ]
    return ResponseEnvelope.ok(MultiUploadResultSchema(results=results))


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
    service = IngestionService(session)
    documents, total = await service.list_documents(corpus_id=corpus_id, page=page, page_size=page_size)
    return ResponseEnvelope.ok(
        Page(
            items=[CorpusDocumentSchema.model_validate(d) for d in documents],
            meta=PageMeta(total=total, page=page, page_size=page_size, pages=_pages(total, page_size)),
        )
    )


@router.get(
    "/corpora/{corpus_id}/documents/{document_id}",
    response_model=ResponseEnvelope[CorpusDocumentContentSchema],
)
async def get_document_content(
    corpus_id: uuid.UUID,
    document_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[CorpusDocumentContentSchema]:
    """Fetch the full content of a single document within a corpus."""
    service = IngestionService(session)
    doc = await service.get_document(corpus_id=corpus_id, document_id=document_id)
    return ResponseEnvelope.ok(CorpusDocumentContentSchema.model_validate(doc))
