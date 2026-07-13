from __future__ import annotations

from collections import defaultdict
from typing import Any
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Codebook,
    CodebookApplicationRun,
    CorpusDocument,
    DemographicFiles,
    DemographicRow,
    DocumentCoding,
    Theme,
    ThemeAssignment,
)
from app.schemas.theme_views import (
    DemographicGroupStat,
    ThemeDemographicBreakdownResponse,
    ThemeDimensionBreakdown,
)
from app.services.theme_graph import ThemeNotFoundError
from app.services.theme_hierarchy import load_descendants_and_self

# The username column links a demographic row to a transcript; it is an
# identifier, not a demographic variable, so it is never offered as a dimension.
USERNAME_COLUMN = "username"

# Bucket used for coded interviews that have no demographic link, or whose
# linked row has no value for the selected dimension. Surfaced (count 0+),
# never silently dropped.
NOT_SPECIFIED_LABEL = "Not specified"

DEFAULT_SMALL_SAMPLE_THRESHOLD = 5


class ThemeDemographicBreakdownService:
    """Break a single theme's frequency down by demographic dimensions.

    The population for a breakdown is the set of interviews coded in the
    selected application run. For each dimension, those interviews are grouped by
    their linked demographic value; within each group we report how many had the
    theme present (absolute count) and the share of the group (percentage).
    Aggregation happens in Python so it stays portable across PostgreSQL (prod)
    and SQLite (tests) without relying on JSON SQL operators.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        small_sample_threshold: int = DEFAULT_SMALL_SAMPLE_THRESHOLD,
    ) -> None:
        self._session = session
        self._small_sample_threshold = small_sample_threshold

    async def list_available_dimensions(self, *, corpus_id: UUID) -> list[str]:
        """Demographic variables present in the uploaded data for one corpus.

        Derived from every demographic file's columns, with the username
        identifier removed. Order of first appearance is preserved and
        duplicates across files are collapsed.
        """
        rows = (
            await self._session.execute(
                select(DemographicFiles.original_columns).where(
                    DemographicFiles.corpus_id == corpus_id
                )
            )
        ).scalars().all()

        dimensions: list[str] = []
        for columns in rows:
            for column in columns or []:
                if column != USERNAME_COLUMN and column not in dimensions:
                    dimensions.append(column)
        return dimensions

    async def get_theme_breakdown(
        self,
        *,
        codebook_id: UUID,
        theme_id: UUID,
        dimensions: list[str],
        application_run_id: UUID | None = None,
    ) -> ThemeDemographicBreakdownResponse:
        corpus_id = await self._resolve_codebook_corpus(codebook_id=codebook_id)
        await self._ensure_node_in_codebook(codebook_id=codebook_id, node_id=theme_id)
        run_id = await self._resolve_run_id(
            codebook_id=codebook_id, application_run_id=application_run_id
        )

        available = await self.list_available_dimensions(corpus_id=corpus_id)
        # Only honor dimensions that actually exist in the data; keep the
        # caller's order and drop unknown/duplicate names.
        selected: list[str] = []
        for dimension in dimensions:
            if dimension in available and dimension not in selected:
                selected.append(dimension)

        if run_id is None or not selected:
            return ThemeDemographicBreakdownResponse(
                theme_id=theme_id,
                application_run_id=run_id,
                dimensions=[
                    ThemeDimensionBreakdown(dimension=dimension, groups=[])
                    for dimension in selected
                ],
            )

        population = await self._load_population(run_id=run_id)
        theme_ids = await load_descendants_and_self(
            self._session, codebook_id=codebook_id, theme_id=theme_id
        )
        present_document_ids = await self._load_present_document_ids(
            run_id=run_id, node_ids=theme_ids
        )

        return ThemeDemographicBreakdownResponse(
            theme_id=theme_id,
            application_run_id=run_id,
            dimensions=[
                self._build_dimension_breakdown(
                    dimension=dimension,
                    population=population,
                    present_document_ids=present_document_ids,
                )
                for dimension in selected
            ],
        )

    async def _resolve_codebook_corpus(self, *, codebook_id: UUID) -> UUID:
        corpus_id = (
            await self._session.execute(
                select(Codebook.corpus_id).where(Codebook.id == codebook_id)
            )
        ).scalar_one_or_none()
        if corpus_id is None:
            raise ThemeNotFoundError(f"Codebook '{codebook_id}' not found.")
        return corpus_id

    async def _ensure_node_in_codebook(self, *, codebook_id: UUID, node_id: UUID) -> None:
        from app.models import Code

        theme_row = (
            await self._session.execute(
                select(Theme.id).where(
                    Theme.id == node_id,
                    Theme.codebook_id == codebook_id,
                )
            )
        ).scalar_one_or_none()
        if theme_row is not None:
            return

        code_row = (
            await self._session.execute(
                select(Code.id).where(
                    Code.id == node_id,
                    Code.codebook_id == codebook_id,
                )
            )
        ).scalar_one_or_none()
        if code_row is not None:
            return

        raise ThemeNotFoundError(
            f"Theme/Code '{node_id}' not found in codebook '{codebook_id}'."
        )

    async def _resolve_run_id(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None,
    ) -> UUID | None:
        if application_run_id is not None:
            run = await self._session.get(CodebookApplicationRun, application_run_id)
            if run is None or run.codebook_id != codebook_id:
                raise ThemeNotFoundError(
                    f"Codebook application run '{application_run_id}' not found "
                    f"for codebook '{codebook_id}'."
                )
            return run.id

        stmt = (
            select(CodebookApplicationRun.id)
            .where(
                CodebookApplicationRun.codebook_id == codebook_id,
                CodebookApplicationRun.status == "succeeded",
            )
            .order_by(
                desc(CodebookApplicationRun.finished_at),
                desc(CodebookApplicationRun.created_at),
            )
            .limit(1)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def _load_population(
        self, *, run_id: UUID
    ) -> list[tuple[UUID, dict[str, Any] | None]]:
        """Every interview coded in the run, paired with its demographic data.

        One coding row exists per (run, document), so each interview appears once.
        Interviews with no demographic link yield ``None`` data.
        """
        rows = (
            await self._session.execute(
                select(DocumentCoding.document_id, DemographicRow.data)
                .join(CorpusDocument, DocumentCoding.document_id == CorpusDocument.id)
                .outerjoin(
                    DemographicRow,
                    CorpusDocument.demographic_row_id == DemographicRow.id,
                )
                .where(DocumentCoding.application_run_id == run_id)
            )
        ).all()
        return [(row.document_id, row.data) for row in rows]

    async def _load_present_document_ids(
        self, *, run_id: UUID, node_ids: set[UUID]
    ) -> set[UUID]:
        from sqlalchemy import union

        from app.models import CodeAssignment

        theme_stmt = select(DocumentCoding.document_id).join(
            ThemeAssignment,
            ThemeAssignment.document_coding_id == DocumentCoding.id,
        ).where(
            DocumentCoding.application_run_id == run_id,
            ThemeAssignment.theme_id.in_(node_ids),
            ThemeAssignment.is_present.is_(True),
        )

        code_stmt = select(DocumentCoding.document_id).join(
            CodeAssignment,
            CodeAssignment.document_coding_id == DocumentCoding.id,
        ).where(
            DocumentCoding.application_run_id == run_id,
            CodeAssignment.code_id.in_(node_ids),
        )

        rows = (
            await self._session.execute(union(theme_stmt, code_stmt))
        ).scalars().all()
        return set(rows)

    def _build_dimension_breakdown(
        self,
        *,
        dimension: str,
        population: list[tuple[UUID, dict[str, Any] | None]],
        present_document_ids: set[UUID],
    ) -> ThemeDimensionBreakdown:
        group_total: dict[str, int] = defaultdict(int)
        group_present: dict[str, int] = defaultdict(int)

        for document_id, data in population:
            value = self._group_value(data, dimension)
            group_total[value] += 1
            if document_id in present_document_ids:
                group_present[value] += 1

        groups = [
            DemographicGroupStat(
                group_value=value,
                present_count=group_present.get(value, 0),
                group_total=group_total[value],
                percentage=self._to_percentage(
                    present=group_present.get(value, 0),
                    total=group_total[value],
                ),
                small_sample=0 < group_total[value] < self._small_sample_threshold,
            )
            for value in sorted(group_total, key=self._group_sort_key)
        ]
        return ThemeDimensionBreakdown(dimension=dimension, groups=groups)

    @staticmethod
    def _group_value(data: dict[str, Any] | None, dimension: str) -> str:
        if not data:
            return NOT_SPECIFIED_LABEL
        raw = data.get(dimension)
        if raw is None:
            return NOT_SPECIFIED_LABEL
        text = str(raw).strip()
        return text or NOT_SPECIFIED_LABEL

    @staticmethod
    def _group_sort_key(value: str) -> tuple[bool, str]:
        # Keep the catch-all bucket last; order real groups alphabetically.
        return (value == NOT_SPECIFIED_LABEL, value.lower())

    @staticmethod
    def _to_percentage(*, present: int, total: int) -> float:
        if total <= 0:
            return 0.0
        return (present / total) * 100.0
