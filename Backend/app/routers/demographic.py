import uuid
import math

from fastapi import APIRouter, Form, UploadFile

from app.dependencies import AppSettings, DbSession
from app.exceptions import UnprocessableError
from app.schemas import Page, PageMeta, ResponseEnvelope
from app.schemas.demographic import (
    DemographicFileSummary,
    DemographicRowSchema,
    ImportDemographicResponse,
    LinkingSummary,
    UploadDemographicConfirmResponse,
)
from app.services.demographic import DemographicService
from app.services.linking import auto_link_demographics

router = APIRouter(prefix="/demographic/{corpus_id}", tags=["demographic"])

def _pages(total: int, page_size: int) -> int:
    return math.ceil(total / page_size) if total > 0 else 0


@router.post(
    "/upload",
    response_model=ResponseEnvelope[ImportDemographicResponse],
    status_code=201,
)
async def upload_demographic_data(
    corpus_id: uuid.UUID,
    file: UploadFile,
    session: DbSession,
    settings: AppSettings,
    name: str | None = Form(default=None),
) -> ResponseEnvelope[ImportDemographicResponse]:
    service = DemographicService(session, settings)
    try:
        response = await service.upload_demographic_data(
            corpus_id=corpus_id,
            file=file,
            name=name,
            max_bytes=settings.MAX_UPLOAD_BYTES,
        )
    except UnprocessableError as exc:
        return ResponseEnvelope[ImportDemographicResponse].fail(
            error="UnprocessableError",
            detail=str(exc),
        )

    return ResponseEnvelope[ImportDemographicResponse].ok(data=response)


@router.post(
    "/confirm",
    response_model=ResponseEnvelope[UploadDemographicConfirmResponse],
    status_code=201,
)
async def confirm_demographic_upload(
    corpus_id: uuid.UUID,
    import_id: uuid.UUID,
    confirm: bool,
    settings: AppSettings,
    session: DbSession,
) -> ResponseEnvelope[UploadDemographicConfirmResponse]:
    service = DemographicService(session, settings)
    try:
        response = await service.confirm_demographic_upload(
            corpus_id=corpus_id,
            import_id=import_id,
            confirm=confirm,
        )
    except UnprocessableError as exc:
        return ResponseEnvelope[UploadDemographicConfirmResponse].fail(
            error="UnprocessableError",
            detail=str(exc),
        )

    return ResponseEnvelope[UploadDemographicConfirmResponse].ok(data=response)


@router.get(
    "/files",
    response_model=ResponseEnvelope[Page[DemographicFileSummary]],
)
async def list_demographic_files(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
    page: int = 1,
    page_size: int = 20,
) -> ResponseEnvelope[Page[DemographicFileSummary]]:
    service = DemographicService(session, settings)
    try:
        items, total = await service.list_files(corpus_id=corpus_id, page=page, page_size=page_size)
    except UnprocessableError as exc:
        return ResponseEnvelope[Page[DemographicFileSummary]].fail(
            error="UnprocessableError",
            detail=str(exc),
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
)
async def list_demographic_rows(
    corpus_id: uuid.UUID,
    session: DbSession,
    settings: AppSettings,
    demographic_file_id: uuid.UUID | None = None,
    page: int = 1,
    page_size: int = 20,
) -> ResponseEnvelope[Page[DemographicRowSchema]]:
    service = DemographicService(session, settings)
    try:
        items, total = await service.list_rows(
            corpus_id=corpus_id,
            demographic_file_id=demographic_file_id,
            page=page,
            page_size=page_size,
        )
    except UnprocessableError as exc:
        return ResponseEnvelope[Page[DemographicRowSchema]].fail(
            error="UnprocessableError",
            detail=str(exc),
        )

    return ResponseEnvelope[Page[DemographicRowSchema]].ok(
        data=Page(
            items=items,
            meta=PageMeta(total=total, page=page, page_size=page_size, pages=_pages(total, page_size)),
        )
    )


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
        await auto_link_demographics(session, corpus_id)
        summary = await service.get_link_summary(corpus_id)
    except UnprocessableError as exc:
        return ResponseEnvelope[LinkingSummary].fail(
            error="UnprocessableError",
            detail=str(exc),
        )
    return ResponseEnvelope[LinkingSummary].ok(data=summary)
