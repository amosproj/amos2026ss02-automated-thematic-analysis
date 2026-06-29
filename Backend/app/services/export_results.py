from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError
from app.models import (
    CodebookApplicationRun,
    CorpusDocument,
    DemographicFiles,
    DemographicRow,
    DocumentCoding,
    Theme,
    ThemeAssignment,
    ThemeHierarchyRelationship,
)


@dataclass
class _ExportRow:
    """One coded segment = one CSV line (a theme matched to a quote)."""

    theme_label: str
    parent_label: str | None
    theme_description: str | None
    participant_id: str
    quote: str
    # Pre-projected demo values in original_columns order — avoids per-row dict lookup in writer.
    demographics: tuple[str, ...]


@dataclass
class ExportData:

    rows: list[_ExportRow]
    demo_columns: list[str]


class RunExportService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def fetch_rows(self, run_id: UUID) -> ExportData:

        run = await self._session.get(CodebookApplicationRun, run_id)
        if run is None:
            raise NotFoundError(f"Codebook application run '{run_id}' not found")

        corpus_id = run.corpus_id

        # Core join, like ThemeQuotesService.list_theme_quotes
        rows = (await self._session.execute(
            select(
                Theme.id, Theme.label, Theme.description,
                ThemeAssignment.quote, ThemeAssignment.created_at,
                DemographicRow.interviewee_id, DemographicRow.data,
                CorpusDocument.title,
            )
            .select_from(ThemeAssignment)
            .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
            .join(Theme, ThemeAssignment.theme_id == Theme.id)
            .join(CorpusDocument, DocumentCoding.document_id == CorpusDocument.id)
            .outerjoin(DemographicRow, CorpusDocument.demographic_row_id == DemographicRow.id)
            .where(
                DocumentCoding.application_run_id == run_id,
                ThemeAssignment.is_present.is_(True),
                ThemeAssignment.quote.is_not(None),
            )
            .order_by(Theme.label, ThemeAssignment.created_at)
        )).all()

        #    the LEFT JOIN to DemographicRow keeps quotes from transcripts that were never linked to a demographic row (interviewee_id == None).

        # Report the behavior: if the transcript was never linked to a demographic row, the id will be None, and the demographics dict will be empty.
        # If the transcript was linked to a demographic row, but the row has no data, the id will be present, but the demographics dict will still be empty.

        #  A theme with no active parent simply gets parent_label = None.
        parent_label_by_child = {
            row.child_theme_id: row.parent_label
            for row in (
                await self._session.execute(
                    select(
                        ThemeHierarchyRelationship.child_theme_id,
                        Theme.label.label("parent_label"),
                    )
                    .join(Theme, ThemeHierarchyRelationship.parent_theme_id == Theme.id)
                    .where(
                        ThemeHierarchyRelationship.codebook_id == run.codebook_id,
                        ThemeHierarchyRelationship.is_active.is_(True),
                    )
                )
            ).all()
        }


        original_columns = (
            await self._session.execute(
                select(DemographicFiles.original_columns)
                .where(DemographicFiles.corpus_id == corpus_id)
                .order_by(DemographicFiles.created_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()

        demo_columns: list[str] = list(original_columns) if original_columns is not None else []

        export_rows = [
            _ExportRow(
                theme_label=row.label,
                parent_label=parent_label_by_child.get(row.id),
                theme_description=row.description,
                # Fall back to the document title when the transcript has no linked demographic row.
                participant_id=row.interviewee_id or row.title,
                quote=row.quote,
                demographics=tuple(str((row.data or {}).get(col, "")) for col in demo_columns),
            )
            for row in rows
        ]

        return ExportData(rows=export_rows, demo_columns=demo_columns)


    def to_theme_based_csv(self, data: ExportData) -> str:
        """File 1: one row per tagged quote, grouped by theme.

        Header: Theme Name, Parent Theme, Theme Description, Participant ID, Quote
        """
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["Theme Name", "Parent Theme", "Theme Description", "Participant ID", "Quote"])
        for row in data.rows:
            writer.writerow([
                row.theme_label,
                row.parent_label or "",
                row.theme_description or "",
                row.participant_id,
                row.quote,
            ])
        return buffer.getvalue()

    def to_participant_based_csv(self, data: ExportData) -> str:
        """File 2: demographics repeated per quote, one row per tagged quote.

        Header: Participant ID, <demo columns…>, Theme Name, Quote
        """
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["Participant ID", *data.demo_columns, "Theme Name", "Quote"])
        for row in data.rows:
            writer.writerow([row.participant_id, *row.demographics, row.theme_label, row.quote])
        return buffer.getvalue()
