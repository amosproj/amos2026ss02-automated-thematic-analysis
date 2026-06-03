import csv
import datetime
import io
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import UploadFile
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.exceptions import NotFoundError, UnprocessableError
from app.models.demographic import DemographicFiles, DemographicRow
from app.models.ingestion import CorpusDocument
from app.schemas.demographic import (
    DemographicFileSummary,
    DemographicRowSchema,
    ImportDemographicPreview,
    ImportDemographicResponse,
    LinkingSummary,
    TranscriptLinkStatus,
    UploadDemographicConfirmResponse,
)
from app.services.ingestion import IngestionService
from app.services.linking import auto_link_demographics
from app.services.upload_parsers import get_extension

SUPPORTED_DEMOGRAPHIC_UPLOAD_EXTENSIONS = frozenset({".csv"})


@dataclass
class ParsedDemographicRow:
    interviewee_id: str
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
        self._ingestion_service = IngestionService(session)

    @staticmethod
    def _coerce_uuid(value: uuid.UUID, field_name: str) -> uuid.UUID:
        if isinstance(value, uuid.UUID):
            return value
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError, AttributeError) as exc:
            raise UnprocessableError(f"Invalid {field_name}") from exc

    def _safe_demographic_path(
        self,
        corpus_id: uuid.UUID,
        import_id: uuid.UUID,
        suffix: str,
    ) -> Path:
        parsed_corpus_id = self._coerce_uuid(corpus_id, "corpus_id")
        parsed_import_id = self._coerce_uuid(import_id, "import_id")
        root = os.path.realpath(os.path.join(self._settings.UPLOADS_DIR, "demographic"))
        candidate = os.path.realpath(
            os.path.join(root, str(parsed_corpus_id), f"{parsed_import_id}{suffix}")
        )
        if os.path.commonpath([root, candidate]) != root:
            raise UnprocessableError("Invalid pending demographic upload path")
        return Path(candidate)

    def _get_out_file_path(self, corpus_id: uuid.UUID, import_id: uuid.UUID) -> Path:
        return self._safe_demographic_path(corpus_id, import_id, ".csv")

    def _get_out_meta_path(self, corpus_id: uuid.UUID, import_id: uuid.UUID) -> Path:
        return self._safe_demographic_path(corpus_id, import_id, ".meta.json")

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

    @staticmethod
    def _detect_csv_delimiter(csv_text: str) -> str:
        sample = csv_text[:4096]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            if dialect.delimiter in {";", ","}:
                return dialect.delimiter
        except csv.Error:
            pass

        header = csv_text.splitlines()[0] if csv_text.splitlines() else ""
        semicolons = header.count(";")
        commas = header.count(",")
        return "," if commas > semicolons else ";"

    def _parse_demographic_csv(self, filename: str, content: bytes) -> ParsedDemographicCsv:
        csv_text = self._decode_csv_bytes(filename, content)
        text_stream = io.StringIO(csv_text)
        delimiter = self._detect_csv_delimiter(csv_text)
        reader = csv.DictReader(text_stream, delimiter=delimiter, restkey="__extra__")
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

        if not fieldnames:
            raise UnprocessableError(f"'{filename}': CSV has no header row")
        if "username" not in fieldnames:
            raise UnprocessableError(
                f"'{filename}': CSV must include 'username' column"
            )
        if len(fieldnames) < 2:
            raise UnprocessableError(
                f"'{filename}': CSV must contain at least 2 columns for demographic import.\n"
                f"One username and one demographic data column."
            )
        if not rows:
            raise UnprocessableError(f"'{filename}': CSV contains no data rows")

        parsed_rows: list[ParsedDemographicRow] = []
        for row_index, row in enumerate(rows, start=1):
            if "__extra__" in row:
                raise UnprocessableError(
                    f"'{filename}': malformed CSV row at line {row_index + 1}: too many columns"
                )

            raw_interviewee_id = (row.get("username") or "").strip()
            if not raw_interviewee_id:
                raise UnprocessableError(
                    f"'{filename}': invalid username at line {row_index + 1}: empty value"
                )

            # Keep dynamic demographic columns and intentionally remove the foreign key
            # to avoid duplicated storage in JSON and dedicated relational column.
            demographic_values = {
                key: value
                for key, value in row.items()
                if key not in {"username", "__extra__"}
            }
            parsed_rows.append(
                ParsedDemographicRow(
                    interviewee_id=raw_interviewee_id,
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

    async def _validate_interviewee_ids_unique(
        self,
        corpus_id: uuid.UUID,
        parsed_rows: list[ParsedDemographicRow],
    ) -> None:
        ids_in_upload = [row.interviewee_id for row in parsed_rows]
        if len(ids_in_upload) != len(set(ids_in_upload)):
            raise UnprocessableError("CSV contains duplicate username values")

        existing = set(
            (
                await self._session.execute(
                    select(DemographicRow.interviewee_id)
                    .where(
                        DemographicRow.corpus_id == corpus_id,
                        DemographicRow.interviewee_id.in_(set(ids_in_upload)),
                    )
                )
            ).scalars()
        )
        if existing:
            raise UnprocessableError(
                f"username already exists: '{sorted(existing)[0]}'. "
                "This usually means the same demographic data was already uploaded for this corpus."
            )

    async def list_files(
        self,
        corpus_id: uuid.UUID,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[DemographicFileSummary], int]:
        await self._validate_corpus(corpus_id)

        count_q = (
            select(func.count())
            .select_from(DemographicFiles)
            .where(DemographicFiles.corpus_id == corpus_id)
        )
        total: int = (await self._session.execute(count_q)).scalar_one()

        offset = (page - 1) * page_size
        rows = await self._session.execute(
            select(
                DemographicFiles,
                func.count(DemographicRow.id).label("rows_total"),
            )
            .outerjoin(DemographicRow, DemographicRow.demographic_file_id == DemographicFiles.id)
            .where(DemographicFiles.corpus_id == corpus_id)
            .group_by(DemographicFiles.id)
            .order_by(DemographicFiles.created_at.desc())
            .offset(offset)
            .limit(page_size)
        )

        items: list[DemographicFileSummary] = []
        for file_row, rows_total in rows.all():
            items.append(
                DemographicFileSummary(
                    id=file_row.id,
                    corpus_id=file_row.corpus_id,
                    name=file_row.name,
                    original_columns=file_row.original_columns,
                    rows_total=int(rows_total or 0),
                    created_at=file_row.created_at,
                    updated_at=file_row.updated_at,
                )
            )
        return items, total

    async def list_rows(
        self,
        corpus_id: uuid.UUID,
        demographic_file_id: uuid.UUID | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> tuple[list[DemographicRowSchema], int]:
        await self._validate_corpus(corpus_id)

        base = (
            select(DemographicRow)
            .join(DemographicFiles, DemographicRow.demographic_file_id == DemographicFiles.id)
            .where(DemographicFiles.corpus_id == corpus_id)
        )
        count_q = (
            select(func.count())
            .select_from(DemographicRow)
            .join(DemographicFiles, DemographicRow.demographic_file_id == DemographicFiles.id)
            .where(DemographicFiles.corpus_id == corpus_id)
        )

        if demographic_file_id is not None:
            file_exists_in_corpus = (
                await self._session.execute(
                    select(DemographicFiles.id).where(
                        DemographicFiles.id == demographic_file_id,
                        DemographicFiles.corpus_id == corpus_id,
                    )
                )
            ).scalar_one_or_none()
            if file_exists_in_corpus is None:
                raise UnprocessableError(
                    f"Demographic file '{demographic_file_id}' does not belong to corpus '{corpus_id}'"
                )

            base = base.where(DemographicRow.demographic_file_id == demographic_file_id)
            count_q = count_q.where(DemographicRow.demographic_file_id == demographic_file_id)

        total: int = (await self._session.execute(count_q)).scalar_one()
        offset = (page - 1) * page_size
        rows = await self._session.execute(
            base.order_by(DemographicRow.demographic_file_id, DemographicRow.row_number)
            .offset(offset)
            .limit(page_size)
        )
        items = [DemographicRowSchema.model_validate(row) for row in rows.scalars().all()]
        return items, total

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
        await self._validate_interviewee_ids_unique(corpus_id, parsed.parsed_rows)

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
        await self._validate_interviewee_ids_unique(corpus_id, parsed.parsed_rows)
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
                    corpus_id=corpus_id,
                    row_number=row_number,
                    interviewee_id=row.interviewee_id,
                    data=row.values,
                )
            )

        try:
            await self._session.commit()
        except Exception as exc:
            await self._session.rollback()
            raise UnprocessableError(f"Could not persist demographic data: {exc}") from exc

        await auto_link_demographics(self._session, corpus_id)

        pending_file_path.unlink(missing_ok=True)
        pending_meta_path.unlink(missing_ok=True)
        return UploadDemographicConfirmResponse(
            import_id=import_id,
            name=final_name,
            status="Demographic data successfully uploaded",
            rows_created=len(parsed.parsed_rows),
        )

    async def get_link_summary(self, corpus_id: uuid.UUID) -> LinkingSummary:
        await self._validate_corpus(corpus_id)

        documents = list(
            (
                await self._session.execute(
                    select(CorpusDocument).where(CorpusDocument.corpus_id == corpus_id)
                )
            ).scalars()
        )

        details = [
            TranscriptLinkStatus(
                document_id=doc.id,
                document_title=doc.title,
                demographic_row_id=doc.demographic_row_id,
                matched=doc.demographic_row_id is not None,
            )
            for doc in documents
        ]

        matched_count = sum(1 for d in details if d.matched)
        return LinkingSummary(
            total_transcripts=len(details),
            matched=matched_count,
            details=details,
        )
