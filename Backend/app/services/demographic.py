import csv
import datetime
import io
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.exceptions import NotFoundError, UnprocessableError
from app.models.demographic import DemographicFiles, DemographicRow
from app.models.ingestion import CorpusDocument
from app.schemas.demographic import (
    ImportDemographicPreview,
    ImportDemographicResponse,
    UploadDemographicConfirmResponse,
)
from app.services.ingestion import IngestionService
from app.services.upload_parsers import get_extension


SUPPORTED_DEMOGRAPHIC_UPLOAD_EXTENSIONS = frozenset({".csv"})


@dataclass
class ParsedDemographicRow:
    corpus_document_id: uuid.UUID
    values: dict[str, Any]


@dataclass
class ParsedDemographicCsv:
    original_columns: list[str]
    rows: list[dict[str, Any]]
    parsed_rows: list[ParsedDemographicRow]


class DemographicService:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self._session = session
        self._settings = settings
        self._ingestion_service = IngestionService(session, settings)

    def _get_out_file_path(self, corpus_id: uuid.UUID, import_id: uuid.UUID) -> Path:
        return Path(
            self._settings.UPLOADS_DIR,
            "demographic",
            str(corpus_id),
            f"{import_id}.csv",
        )

    def _get_out_meta_path(self, corpus_id: uuid.UUID, import_id: uuid.UUID) -> Path:
        return Path(
            self._settings.UPLOADS_DIR,
            "demographic",
            str(corpus_id),
            f"{import_id}.meta.json",
        )

    @staticmethod
    def _normalize_demographic_name(name: str) -> str:
        normalized = name.strip()
        if not normalized:
            raise UnprocessableError("Demographic file name cannot be empty")
        if len(normalized) > 255:
            raise UnprocessableError("Demographic file name exceeds 255 characters")
        return normalized

    async def _resolve_name_conflict(self, corpus_id: uuid.UUID, desired_name: str) -> str:
        desired_name = self._normalize_demographic_name(desired_name)
        existing_names = set(
            (
                await self._session.execute(
                    select(DemographicFiles.name).where(DemographicFiles.corpus_id == corpus_id)
                )
            ).scalars()
        )
        if desired_name not in existing_names:
            return desired_name

        n = 2
        while f"{desired_name} ({n})" in existing_names:
            n += 1
        return f"{desired_name} ({n})"

    def _validate_file_extension(self, filename: str) -> None:
        ext = get_extension(filename)
        if ext not in SUPPORTED_DEMOGRAPHIC_UPLOAD_EXTENSIONS:
            raise UnprocessableError(
                f"Unsupported file extension '{ext}'. "
                f"Supported: {', '.join(sorted(SUPPORTED_DEMOGRAPHIC_UPLOAD_EXTENSIONS))}"
            )

    @staticmethod
    def _decode_csv_bytes(filename: str, content: bytes) -> str:
        try:
            return content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise UnprocessableError(f"Could not decode '{filename}' as UTF-8") from exc

    def _parse_demographic_csv(self, filename: str, content: bytes) -> ParsedDemographicCsv:
        text_stream = io.StringIO(self._decode_csv_bytes(filename, content))
        reader = csv.DictReader(text_stream, restkey="__extra__")
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

        if not fieldnames:
            raise UnprocessableError(f"'{filename}': CSV has no header row")
        if "corpus_document_id" not in fieldnames:
            raise UnprocessableError(
                f"'{filename}': CSV must include 'corpus_document_id' column"
            )
        if len(fieldnames) < 2:
            raise UnprocessableError(
                f"'{filename}': CSV must contain at least 2 columns for demographic import.\n"
                f"One corpus_document_id and one demographic data row."
            )
        if not rows:
            raise UnprocessableError(f"'{filename}': CSV contains no data rows")

        parsed_rows: list[ParsedDemographicRow] = []
        for row_index, row in enumerate(rows, start=1):
            if "__extra__" in row:
                raise UnprocessableError(
                    f"'{filename}': malformed CSV row at line {row_index + 1}: too many columns"
                )

            raw_document_id = (row.get("corpus_document_id") or "").strip()
            if not raw_document_id:
                raise UnprocessableError(
                    f"'{filename}': invalid corpus_document_id at line {row_index + 1}: empty value"
                )

            try:
                parsed_document_id = uuid.UUID(raw_document_id)
            except ValueError:
                raise UnprocessableError(
                    f"'{filename}': invalid corpus_document_id at line {row_index + 1}: '{raw_document_id}'"
                ) from None

            # Keep dynamic demographic columns and intentionally remove the foreign key
            # to avoid duplicated storage in JSON and dedicated relational column.
            demographic_values = {
                key: value
                for key, value in row.items()
                if key not in {"corpus_document_id", "__extra__"}
            }
            parsed_rows.append(
                ParsedDemographicRow(
                    corpus_document_id=parsed_document_id,
                    values=demographic_values,
                )
            )

        return ParsedDemographicCsv(
            original_columns=fieldnames,
            rows=rows,
            parsed_rows=parsed_rows,
        )

    async def _validate_corpus(self, corpus_id: uuid.UUID) -> None:
        try:
            await self._ingestion_service.get_corpus(corpus_id=corpus_id)
        except NotFoundError:
            raise UnprocessableError(f"Corpus with id '{corpus_id}' does not exist") from None

    async def _validate_corpus_document_ids(
        self,
        corpus_id: uuid.UUID,
        parsed_rows: list[ParsedDemographicRow],
        filename: str,
    ) -> None:
        unique_ids = {row.corpus_document_id for row in parsed_rows}
        existing_document_ids = set(
            (
                await self._session.execute(
                    select(CorpusDocument.id).where(
                        CorpusDocument.corpus_id == corpus_id,
                        CorpusDocument.id.in_(unique_ids),
                    )
                )
            ).scalars()
        )
        missing_ids = [doc_id for doc_id in unique_ids if doc_id not in existing_document_ids]
        if missing_ids:
            missing = str(missing_ids[0])
            raise UnprocessableError(
                f"'{filename}': invalid corpus_document_id not present in corpus '{corpus_id}': '{missing}'"
            )

    async def upload_demographic_data(
        self,
        corpus_id: uuid.UUID,
        file: UploadFile,
        name: str | None,
        max_bytes: int,
    ) -> ImportDemographicResponse:
        filename = file.filename or ""
        self._validate_file_extension(filename)
        requested_name = name or Path(filename).stem or "demographic"
        normalized_name = self._normalize_demographic_name(requested_name)

        content = await file.read()
        if len(content) == 0:
            raise UnprocessableError(f"'{filename}': file is empty")
        if len(content) > max_bytes:
            raise UnprocessableError(
                f"'{filename}': file exceeds maximum size of {max_bytes} bytes"
            )

        parsed = self._parse_demographic_csv(filename, content)
        await self._validate_corpus(corpus_id)
        await self._validate_corpus_document_ids(corpus_id, parsed.parsed_rows, filename)

        import_id = uuid.uuid4()
        out_file_path = self._get_out_file_path(corpus_id, import_id)
        out_meta_path = self._get_out_meta_path(corpus_id, import_id)
        out_file_path.parent.mkdir(parents=True, exist_ok=True)
        out_file_path.write_bytes(content)
        out_meta_path.write_text(json.dumps({"name": normalized_name}), encoding="utf-8")

        return ImportDemographicResponse(
            import_id=import_id,
            name=normalized_name,
            status="pending",
            preview=ImportDemographicPreview(
                rows_detected=len(parsed.rows),
                columns_detected=len(parsed.original_columns),
                sample_rows=parsed.rows[:10],
            ),
            expires_at=datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(seconds=self._settings.DEMOGRAPHIC_UPLOAD_TTL_SECONDS),
        )

    async def confirm_demographic_upload(
        self,
        corpus_id: uuid.UUID,
        import_id: uuid.UUID,
        confirm: bool,
    ) -> UploadDemographicConfirmResponse:
        pending_file_path = self._get_out_file_path(corpus_id, import_id)
        pending_meta_path = self._get_out_meta_path(corpus_id, import_id)
        if not pending_file_path.exists():
            raise UnprocessableError(
                f"No pending upload found for import_id '{import_id}'\n"
                f"Maybe it expired? Pending uploads must be confirmed within "
                f"{self._settings.DEMOGRAPHIC_UPLOAD_TTL_SECONDS} seconds of the initial upload."
            )

        if not confirm:
            cancelled_name = "demographic"
            if pending_meta_path.exists():
                try:
                    metadata = json.loads(pending_meta_path.read_text(encoding="utf-8"))
                    cancelled_name = self._normalize_demographic_name(str(metadata.get("name") or ""))
                except Exception:
                    cancelled_name = "demographic"
            pending_file_path.unlink(missing_ok=True)
            pending_meta_path.unlink(missing_ok=True)
            return UploadDemographicConfirmResponse(
                import_id=import_id,
                name=cancelled_name,
                status="Upload cancelled by user",
                rows_created=0,
            )

        await self._validate_corpus(corpus_id)

        content = pending_file_path.read_bytes()
        parsed = self._parse_demographic_csv(filename=pending_file_path.name, content=content)
        await self._validate_corpus_document_ids(corpus_id, parsed.parsed_rows, pending_file_path.name)
        if pending_meta_path.exists():
            metadata = json.loads(pending_meta_path.read_text(encoding="utf-8"))
            desired_name = self._normalize_demographic_name(str(metadata.get("name") or ""))
        else:
            desired_name = "demographic"
        final_name = await self._resolve_name_conflict(corpus_id, desired_name)

        self._session.add(
            DemographicFiles(
                id=import_id,
                name=final_name,
                corpus_id=corpus_id,
                original_columns=parsed.original_columns,
            )
        )
        for row_number, row in enumerate(parsed.parsed_rows, start=1):
            self._session.add(
                DemographicRow(
                    demographic_file_id=import_id,
                    row_number=row_number,
                    corpus_document_id=row.corpus_document_id,
                    data=row.values,
                )
            )

        try:
            await self._session.commit()
        except Exception as exc:
            await self._session.rollback()
            raise UnprocessableError(f"Could not persist demographic data: {exc}") from exc

        pending_file_path.unlink(missing_ok=True)
        pending_meta_path.unlink(missing_ok=True)
        return UploadDemographicConfirmResponse(
            import_id=import_id,
            name=final_name,
            status="Demographic data successfully uploaded",
            rows_created=len(parsed.parsed_rows),
        )
