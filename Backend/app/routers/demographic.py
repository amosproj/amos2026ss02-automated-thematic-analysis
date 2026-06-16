import math
import uuid

from fastapi import APIRouter, File, Form, Query, UploadFile
from fastapi.responses import JSONResponse

from app.dependencies import AppSettings, DbSession
from app.exceptions import NotFoundError, UnprocessableError
from app.schemas import Page, PageMeta, ResponseEnvelope
from app.schemas.demographic import (
    DemographicFileSummary,
    DemographicRowSchema,
    ImportDemographicResponse,
    LinkingSummary,
    LinkRequest,
    UploadDemographicConfirmResponse,
)
from app.services.demographic import DemographicService
from app.services.linking import set_document_link

router = APIRouter(prefix="/demographic/{corpus_id}", tags=["demographic"])

def _pages(total: int, page_size: int) -> int:
    """Calculate total number of pages for pagination metadata."""
    return math.ceil(total / page_size) if total > 0 else 0


@router.post(
    "/upload",
    response_model=ResponseEnvelope[ImportDemographicResponse],
    status_code=201,
    summary="Upload demographic CSV (preview)",
    description=(
        "Validate a demographic CSV file for one corpus and return a preview. "
        "The upload stays pending until `/confirm` is called."
    ),
)
async def upload_demographic_data(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
    file: UploadFile = File(..., description="Demographic CSV file to validate and preview."),
    name: str | None = Form(
        default=None,
        description="Optional logical import name. Defaults to the uploaded filename stem.",
    ),
) -> ResponseEnvelope[ImportDemographicResponse] | JSONResponse:
    """Validate CSV structure and create a pending import with preview metadata."""
    service = DemographicService(session, settings)
    try:
        response = await service.upload_demographic_data(
            corpus_id=corpus_id,
            file=file,
            name=name,
            max_bytes=settings.MAX_UPLOAD_BYTES,
        )
    except UnprocessableError as exc:
        return JSONResponse(
            status_code=UnprocessableError.status_code,
            content=ResponseEnvelope[ImportDemographicResponse].fail(
                error="UnprocessableError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    return ResponseEnvelope[ImportDemographicResponse].ok(data=response)


@router.post(
    "/confirm",
    response_model=ResponseEnvelope[UploadDemographicConfirmResponse],
    status_code=201,
    summary="Confirm or cancel pending demographic upload",
    description=(
        "Finalize a pending upload created by `/upload` and persist rows to the database, "
        "or cancel and discard the pending file."
    ),
)
async def confirm_demographic_upload(
    corpus_id: uuid.UUID,
    settings: AppSettings,
    session: DbSession,
    import_id: uuid.UUID = Query(
        ...,
        description="Import id returned by the upload endpoint.",
    ),
    confirm: bool = Query(
        ...,
        description="Set `true` to persist data, `false` to cancel and delete pending upload.",
    ),
) -> ResponseEnvelope[UploadDemographicConfirmResponse] | JSONResponse:
    """Persist or cancel a previously uploaded demographic CSV."""
    service = DemographicService(session, settings)
    try:
        response = await service.confirm_demographic_upload(
            corpus_id=corpus_id,
            import_id=import_id,
            confirm=confirm,
        )
    except UnprocessableError as exc:
        return JSONResponse(
            status_code=UnprocessableError.status_code,
            content=ResponseEnvelope[UploadDemographicConfirmResponse].fail(
                error="UnprocessableError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    return ResponseEnvelope[UploadDemographicConfirmResponse].ok(data=response)


@router.get(
    "/files",
    response_model=ResponseEnvelope[Page[DemographicFileSummary]],
    summary="List demographic imports",
    description="List confirmed demographic imports for a corpus, including row counts.",
)
async def list_demographic_files(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
    page: int = Query(default=1, ge=1, description="1-based page number."),
    page_size: int = Query(default=20, ge=1, le=200, description="Number of items per page."),
) -> ResponseEnvelope[Page[DemographicFileSummary]] | JSONResponse:
    """Return paginated demographic import metadata for one corpus."""
    service = DemographicService(session, settings)
    try:
        items, total = await service.list_files(corpus_id=corpus_id, page=page, page_size=page_size)
    except UnprocessableError as exc:
        return JSONResponse(
            status_code=UnprocessableError.status_code,
            content=ResponseEnvelope[Page[DemographicFileSummary]].fail(
                error="UnprocessableError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    return ResponseEnvelope[Page[DemographicFileSummary]].ok(
        data=Page(
            items=items,
            meta=PageMeta(total=total, page=page, page_size=page_size, pages=_pages(total, page_size)),
        )
    )


@router.get(
    "/rows",
    response_model=ResponseEnvelope[Page[DemographicRowSchema]],
    summary="List demographic rows",
    description=(
        "List confirmed demographic rows for a corpus. "
        "Optionally filter to one demographic file."
    ),
)
async def list_demographic_rows(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
    demographic_file_id: uuid.UUID | None = Query(
        default=None,
        description="Optional filter for one demographic import id.",
    ),
    page: int = Query(default=1, ge=1, description="1-based page number."),
    page_size: int = Query(default=20, ge=1, le=200, description="Number of items per page."),
) -> ResponseEnvelope[Page[DemographicRowSchema]] | JSONResponse:
    """Return paginated demographic rows for one corpus."""
    service = DemographicService(session, settings)
    try:
        items, total = await service.list_rows(
            corpus_id=corpus_id,
            demographic_file_id=demographic_file_id,
            page=page,
            page_size=page_size,
        )
    except UnprocessableError as exc:
        return JSONResponse(
            status_code=UnprocessableError.status_code,
            content=ResponseEnvelope[Page[DemographicRowSchema]].fail(
                error="UnprocessableError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    return ResponseEnvelope[Page[DemographicRowSchema]].ok(
        data=Page(
            items=items,
            meta=PageMeta(total=total, page=page, page_size=page_size, pages=_pages(total, page_size)),
        )
    )


@router.delete(
    "/files/{file_id}",
    response_model=ResponseEnvelope[None],
    summary="Delete a demographic file",
    description="Delete a demographic file and all of its rows.",
)
async def delete_demographic_file(
    corpus_id: uuid.UUID,
    file_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[None] | JSONResponse:
    """Delete a demographic file and all associated rows."""
    service = DemographicService(session, settings)
    try:
        await service.delete_file(corpus_id=corpus_id, demographic_file_id=file_id)
    except NotFoundError as exc:
        return JSONResponse(
            status_code=NotFoundError.status_code,
            content=ResponseEnvelope[None].fail(
                error="NotFoundError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )
    except UnprocessableError as exc:
        return JSONResponse(
            status_code=UnprocessableError.status_code,
            content=ResponseEnvelope[None].fail(
                error="UnprocessableError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )
    return ResponseEnvelope[None].ok(data=None)


@router.get(
    "/link-summary",
    response_model=ResponseEnvelope[LinkingSummary],
)
async def get_link_summary(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[LinkingSummary]:
    service = DemographicService(session, settings)
    try:
        # Auto-linking runs at ingestion and on demographic confirm; we must not
        # re-run it here. Doing so on every read re-creates links that were
        # manually removed (and can double-link a row after a reassign), silently
        # reverting the manual overrides this endpoint is meant to surface.
        summary = await service.get_link_summary(corpus_id)
    except UnprocessableError as exc:
        return ResponseEnvelope[LinkingSummary].fail(
            error="UnprocessableError",
            detail=str(exc),
        )
    return ResponseEnvelope[LinkingSummary].ok(data=summary)


@router.put(
    "/documents/{document_id}/link",
    response_model=ResponseEnvelope[LinkingSummary],
    summary="Manually link a transcript to a demographic row",
    description=(
        "Set or reassign the demographic row linked to one transcript. "
        "A demographic row maps to at most one transcript, so linking a row that is "
        "already linked elsewhere moves the link. Returns the refreshed linking summary."
    ),
)
async def link_transcript(
    corpus_id: uuid.UUID,
    document_id: uuid.UUID,
    payload: LinkRequest,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[LinkingSummary] | JSONResponse:
    """Manually link (or reassign) a transcript to a demographic row."""
    service = DemographicService(session, settings)
    try:
        await set_document_link(
            session,
            corpus_id=corpus_id,
            document_id=document_id,
            demographic_row_id=payload.demographic_row_id,
        )
        summary = await service.get_link_summary(corpus_id)
    except NotFoundError as exc:
        return JSONResponse(
            status_code=NotFoundError.status_code,
            content=ResponseEnvelope[LinkingSummary].fail(
                error="NotFoundError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )
    except UnprocessableError as exc:
        return JSONResponse(
            status_code=UnprocessableError.status_code,
            content=ResponseEnvelope[LinkingSummary].fail(
                error="UnprocessableError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )
    return ResponseEnvelope[LinkingSummary].ok(data=summary)


@router.delete(
    "/documents/{document_id}/link",
    response_model=ResponseEnvelope[LinkingSummary],
    summary="Remove the demographic link from a transcript",
    description="Clear the demographic row linked to one transcript. Returns the refreshed summary.",
)
async def unlink_transcript(
    corpus_id: uuid.UUID,
    document_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
) -> ResponseEnvelope[LinkingSummary] | JSONResponse:
    """Remove the demographic link from a transcript."""
    service = DemographicService(session, settings)
    try:
        await set_document_link(
            session,
            corpus_id=corpus_id,
            document_id=document_id,
            demographic_row_id=None,
        )
        summary = await service.get_link_summary(corpus_id)
    except NotFoundError as exc:
        return JSONResponse(
            status_code=NotFoundError.status_code,
            content=ResponseEnvelope[LinkingSummary].fail(
                error="NotFoundError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )
    except UnprocessableError as exc:
        return JSONResponse(
            status_code=UnprocessableError.status_code,
            content=ResponseEnvelope[LinkingSummary].fail(
                error="UnprocessableError",
                detail=str(exc),
            ).model_dump(mode="json"),
        )
    return ResponseEnvelope[LinkingSummary].ok(data=summary)
