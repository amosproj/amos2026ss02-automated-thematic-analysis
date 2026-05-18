import uuid

from fastapi import APIRouter, UploadFile

from app.dependencies import AppSettings, DbSession
from app.exceptions import UnprocessableError
from app.schemas import ResponseEnvelope
from app.schemas.demographic import (
    ImportDemographicResponse,
    UploadDemographicConfirmResponse,
)
from app.services.demographic import DemographicService

router = APIRouter(prefix="/demographic/{corpus_id}", tags=["demographic"])


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
) -> ResponseEnvelope[ImportDemographicResponse]:
    service = DemographicService(session, settings)
    try:
        response = await service.upload_demographic_data(
            corpus_id=corpus_id,
            file=file,
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
