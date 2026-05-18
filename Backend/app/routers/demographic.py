import csv
import datetime
import io
import uuid
from pathlib import Path

from fastapi import APIRouter, UploadFile
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.dependencies import DbSession, AppSettings
from app.exceptions import UnprocessableError, NotFoundError
from app.models.demographic import DemographicFiles, DemographicRow
from app.models.ingestion import CorpusDocument
from app.schemas import ResponseEnvelope
from app.schemas.demographic import ImportDemographicResponse, ImportDemographicPreview, \
    UploadDemographicConfirmResponse
from app.services.ingestion import IngestionService

router = APIRouter(prefix="/demographic/{corpus_id}", tags=["demographic"])

def _get_out_file_path(corpus_id: uuid.UUID, import_id: uuid.UUID, settings: AppSettings) -> Path:
    """Construct the file path for storing the uploaded demographic CSV."""
    return Path(
        settings.UPLOADS_DIR,
        "demographic",
        str(corpus_id),
        f"{import_id}.csv",
    )

@router.post(
    "/upload",
    response_model=ResponseEnvelope[ImportDemographicResponse],
    status_code=201,
)
async def upload_demographic_data(
        corpus_id: uuid.UUID,
        file: UploadFile,
        max_bytes: int,
        session: DbSession,
        settings: AppSettings,
) -> ResponseEnvelope[ImportDemographicResponse]:
    """Endpoint to upload a demographic CSV file for a specific corpus.
    The file is validated for type, size, and content before being saved to a temporary location.
    The response includes a preview of the uploaded data and an import ID for the confirmation endpoint.
    The user must then call the confirmation endpoint with the import ID to finalize the import of the demographic
    data. Unconfirmed data will be automatically deleted after a TTL defined in settings."""
    filename = file.filename or ""

    try:
        # Validate file type for csv
        if file.content_type != "text/csv":
            raise UnprocessableError(
                f"Unsupported file extension '{file.content_type}'. "
                f"Supported: csv"
            )

        content = await file.read()
        if len(content) == 0:
            raise UnprocessableError(f"'{filename}': file is empty")
        if len(content) > max_bytes:
            raise UnprocessableError(
                f"'{filename}': file exceeds maximum size of {max_bytes} bytes"
            )

        # Parse CSV content and validate header row
        text_stream = io.StringIO(content.decode("utf-8-sig"))
        reader = csv.DictReader(text_stream)
        rows = list(reader)
        if not reader.fieldnames:
            raise UnprocessableError(f"'{filename}': CSV has no header row")
        if "corpus_document_id" not in reader.fieldnames:
            raise UnprocessableError(
                f"'{filename}': CSV must include 'corpus_document_id' column"
            )

        # Check if file contains rows
        if len(rows) == 0:
            raise UnprocessableError(f"'{filename}': CSV contains no data rows")

        # Check if file contains at least 2 columns (corpus_document_id + at least one demographic column)
        if len(reader.fieldnames) < 2:
            raise UnprocessableError(f"'{filename}': CSV must contain at least 2 columns for demographic import.\n"
                                     f"One corpus_document_id and one demographic data row.")

        # Check if corpus_id exists in the database
        try:
            await IngestionService.get_corpus(corpus_id=corpus_id)
        except NotFoundError:
            raise UnprocessableError(f"Corpus with id '{corpus_id}' does not exist")

        # Validate that all corpus_document_id values are valid UUIDs and exist in the database
        parsed_document_ids: set[uuid.UUID] = set()
        raw_document_ids_in_order: list[str] = []
        for row in rows:
            raw_value = (row.get("corpus_document_id") or "").strip()
            raw_document_ids_in_order.append(raw_value)
            if not raw_value:
                raise UnprocessableError(
                    "Invalid corpus_document_id: empty value"
                )
            try:
                parsed_document_ids.add(uuid.UUID(raw_value))
            except ValueError:
                raise UnprocessableError(
                    f"Invalid corpus_document_id: '{raw_value}'"
                ) from None

        existing_document_ids = set(
            (
                await session.execute(
                    select(CorpusDocument.id).where(
                        CorpusDocument.corpus_id == corpus_id,
                        CorpusDocument.id.in_(parsed_document_ids),
                    )
                )
            ).scalars()
        )
        for raw_value in raw_document_ids_in_order:
            parsed_value = uuid.UUID(raw_value)
            if parsed_value not in existing_document_ids:
                raise UnprocessableError(
                    f"Invalid corpus_document_id: '{raw_value}'"
                )

        # Generate unique import ID and create output path
        import_id = uuid.uuid4()
        out_file_path = _get_out_file_path(corpus_id, import_id, settings)
        out_file_path.parent.mkdir(parents=True, exist_ok=True)

        # Save the uploaded file content to the designated path
        out_file_path.write_bytes(content)

    except UnprocessableError as exc:
        return ResponseEnvelope[ImportDemographicResponse].fail(
            error="UnprocessableError",
            detail=str(exc),
        )

    preview = ImportDemographicPreview(
        rows_detected=len(rows),
        columns_detected=len(reader.fieldnames),
        sample_rows=rows[:10],  # Include up to the first 10 rows as a preview
    )
    response = ImportDemographicResponse(
        import_id=import_id,
        status="pending",
        preview=preview,
        expires_at=datetime.datetime.now(datetime.UTC)
        + datetime.timedelta(seconds=settings.DEMOGRAPHIC_UPLOAD_TTL_SECONDS),
    )

    return ResponseEnvelope[ImportDemographicResponse].ok(
        data=response
    )

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
    """Confirm or cancel a pending demographic data upload. If confirmed, the demographic data from the pending file
    will be processed and stored in the database. If cancelled, the pending file will be deleted and no data will be
    stored."""
    try:
        # Check if the pending upload file exists for the given import_id and corpus_id
        if not _get_out_file_path(corpus_id, import_id, settings).exists():
            raise UnprocessableError(
                f"No pending upload found for import_id '{import_id}'\n"
                f"Maybe it expired? Pending uploads must be confirmed within "
                f""f"{settings.DEMOGRAPHIC_UPLOAD_TTL_SECONDS} seconds of the initial upload."
            )

        if not confirm:
            # If the user does not confirm, delete the pending file and return
            _get_out_file_path(corpus_id, import_id, settings).unlink(missing_ok=True)
            response = UploadDemographicConfirmResponse(
                import_id=import_id,
                status="Upload cancelled by user",
                rows_created=0
            )
            return ResponseEnvelope[UploadDemographicConfirmResponse].ok(
                data=response
            )

    except UnprocessableError as exc:
        return ResponseEnvelope[UploadDemographicConfirmResponse].fail(
            error="UnprocessableError",
            detail=str(exc),
        )

    # Check if corpus_id exists in the database
    try:
        await IngestionService.get_corpus(corpus_id=corpus_id)
    except NotFoundError:
        raise UnprocessableError(f"Corpus with id '{corpus_id}' does not exist")

    # read the pending file and insert demographic data into the database, linked to the corresponding corpus documents
    pending_file_path = _get_out_file_path(corpus_id, import_id, settings)
    content = pending_file_path.read_bytes()
    text_stream = io.StringIO(content.decode("utf-8-sig"))
    reader = csv.DictReader(text_stream)
    original_columns = list(reader.fieldnames or [])

    session.add(
        DemographicFiles(
            id=import_id,
            corpus_id=corpus_id,
            original_columns=original_columns
        )
    )
    rows_created = 0
    for row_number, row in enumerate(reader, start=1):
        demographic_row = DemographicRow(
            demographic_file_id=import_id,
            row_number=row_number,
            data=row,
        )
        session.add(demographic_row)
        rows_created += 1

    response = UploadDemographicConfirmResponse(
        import_id=import_id,
        status="Demographic data successfully uploaded",
        rows_created=rows_created
    )

    return ResponseEnvelope[UploadDemographicConfirmResponse].ok(
        data=response
    )