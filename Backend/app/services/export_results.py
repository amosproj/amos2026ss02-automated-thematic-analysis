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

# Demographic link-key column: surfaced as Participant ID, so it's excluded from
# the demographic data columns to avoid an empty duplicate column.
LINK_KEY_COLUMN = "username"


@dataclass
class _ExportRow:
    """One coded segment = one CSV line (a theme matched to a quote)."""

    theme_label: str
    parent_label: str | None
    theme_description: str | None
    participant_id: str
    quote: str
    demographics: tuple[str, ...] # to avoid per-row dict lookup in writer


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

        # LEFT JOIN to DemographicRow keeps quotes from unlinked transcripts.
        from app.models import Code, CodeAssignment

        theme_rows = (
            await self._session.execute(
                select(
                    Theme.id,
                    Theme.label,
                    Theme.description,
                    ThemeAssignment.quote,
                    ThemeAssignment.created_at,
                    DemographicRow.interviewee_id,
                    DemographicRow.data,
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
            )
        ).all()

        code_rows = (
            await self._session.execute(
                select(
                    Code.id,
                    Code.label,
                    Code.description,
                    CodeAssignment.quote,
                    CodeAssignment.created_at,
                    DemographicRow.interviewee_id,
                    DemographicRow.data,
                    CorpusDocument.title,
                )
                .select_from(CodeAssignment)
                .join(DocumentCoding, CodeAssignment.document_coding_id == DocumentCoding.id)
                .join(Code, CodeAssignment.code_id == Code.id)
                .join(CorpusDocument, DocumentCoding.document_id == CorpusDocument.id)
                .outerjoin(DemographicRow, CorpusDocument.demographic_row_id == DemographicRow.id)
                .where(
                    DocumentCoding.application_run_id == run_id,
                    CodeAssignment.quote.is_not(None),
                )
            )
        ).all()

        rows = list(theme_rows) + list(code_rows)
        rows.sort(key=lambda r: (r.label, r.created_at))

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

        from app.models.code import ThemeCodeRelationship
        for row in (
            await self._session.execute(
                select(
                    ThemeCodeRelationship.code_id,
                    Theme.label.label("parent_label"),
                )
                .join(Theme, ThemeCodeRelationship.theme_id == Theme.id)
                .where(
                    ThemeCodeRelationship.codebook_id == run.codebook_id,
                    ThemeCodeRelationship.is_active.is_(True),
                )
            )
        ).all():
            parent_label_by_child[row.code_id] = row.parent_label


        original_columns = (
            await self._session.execute(
                select(DemographicFiles.original_columns)
                .where(DemographicFiles.corpus_id == run.corpus_id)
                .order_by(DemographicFiles.created_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        demo_columns = [col for col in (original_columns or []) if col != LINK_KEY_COLUMN]

        export_rows = [
            _ExportRow(
                theme_label=row.label,
                parent_label=parent_label_by_child.get(row.id),
                theme_description=row.description,
                # Document title is the fallback when no demographic row is linked.
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
        """File 2: demographics repeated per quote, grouped by participant.

        Sorted by (participant, quote, theme) so a quote tagged with several
        themes stays on consecutive rows.
        """
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["Participant ID", *data.demo_columns, "Theme Name", "Quote"])
        rows = sorted(data.rows, key=lambda r: (r.participant_id, r.quote, r.theme_label))
        for row in rows:
            writer.writerow([row.participant_id, *row.demographics, row.theme_label, row.quote])
        return buffer.getvalue()
