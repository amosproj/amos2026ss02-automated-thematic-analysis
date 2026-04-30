import math
import uuid

from fastapi import APIRouter, UploadFile

from app.dependencies import AppSettings, DbSession
from app.domain.enums import SourceType
from app.exceptions import UnprocessableError
from app.schemas.common import Page, PageMeta, ResponseEnvelope
from app.schemas.ingestion import (
    BulkDocumentIngestRequest,
    CorpusChunkSchema,
    CorpusCreate,
    CorpusDocumentSchema,
    CorpusSchema,
    IngestionResultSchema,
    IngestionRunSchema,
)
from app.services.ingestion import (
    IngestionResult,
    IngestionService,
    parse_csv_upload,
    parse_json_upload,
    parse_text_upload,
)

router = APIRouter(prefix="/ingestion", tags=["ingestion"])

_SUPPORTED_EXTENSIONS = {".txt", ".json", ".csv"}


def _pages(total: int, page_size: int) -> int:
    return math.ceil(total / page_size) if total > 0 else 0


async def _result_schema(
    session: DbSession,
    result: IngestionResult,
) -> IngestionResultSchema:
    await session.refresh(result.run)

    for document in result.documents:
        await session.refresh(document)

    return IngestionResultSchema(
        run=IngestionRunSchema.model_validate(result.run),
        documents=[CorpusDocumentSchema.model_validate(d) for d in result.documents],
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
    project_id: uuid.UUID | None = None,
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


# -------------------------------------
# Document ingestion endpoints


@router.post(
    "/corpora/{corpus_id}/documents/bulk",
    response_model=ResponseEnvelope[IngestionResultSchema],
    status_code=201,
)
async def bulk_ingest_documents(
    corpus_id: uuid.UUID,
    payload: BulkDocumentIngestRequest,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[IngestionResultSchema]:
    service = IngestionService(session, settings)
    result = await service.ingest_documents(
        corpus_id=corpus_id,
        documents=payload.documents,
        source_type=payload.source_type,
    )
    return ResponseEnvelope.ok(await _result_schema(session, result))


@router.post(
    "/corpora/{corpus_id}/upload",
    response_model=ResponseEnvelope[IngestionResultSchema],
    status_code=201,
)
async def upload_documents(
    corpus_id: uuid.UUID,
    file: UploadFile,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[IngestionResultSchema]:
    filename = file.filename or ""
    dot_pos = filename.rfind(".")
    ext = filename[dot_pos:].lower() if dot_pos != -1 else ""

    if ext not in _SUPPORTED_EXTENSIONS:
        raise UnprocessableError(
            f"Unsupported file extension '{ext}'. Supported: {', '.join(sorted(_SUPPORTED_EXTENSIONS))}"
        )

    content = await file.read()

    if ext == ".txt":
        docs = parse_text_upload(filename, content)
        source_type = SourceType.TEXT
    elif ext == ".json":
        docs = parse_json_upload(filename, content)
        source_type = SourceType.JSON
    else:  # .csv
        docs = parse_csv_upload(filename, content)
        source_type = SourceType.CSV

    service = IngestionService(session, settings)
    result = await service.ingest_documents(
        corpus_id=corpus_id,
        documents=docs,
        source_type=source_type,
        filename=filename,
    )
    return ResponseEnvelope.ok(await _result_schema(session, result))


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
    document_id: uuid.UUID | None = None,
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


@router.get("/runs/{run_id}", response_model=ResponseEnvelope[IngestionRunSchema])
async def get_ingestion_run(
    run_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[IngestionRunSchema]:
    service = IngestionService(session, settings)
    run = await service.get_ingestion_run(run_id)
    return ResponseEnvelope.ok(IngestionRunSchema.model_validate(run))
