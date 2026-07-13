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
    DemographicDimensionInfo,
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

# Bounds for the researcher-chosen bin count on a numeric dimension. Below 2
# there is nothing to bin; above 20 the chart stops being readable.
MIN_BIN_COUNT = 2
MAX_BIN_COUNT = 20


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

    async def get_dimension_infos(self, *, corpus_id: UUID) -> list[DemographicDimensionInfo]:
        """Demographic variables for one corpus, each flagged numeric or not.

        A dimension is numeric only when every non-empty value observed for it
        across the corpus parses as a number. Any non-numeric value (or no
        values at all) makes it categorical — a dimension can't be partially
        binnable.
        """
        names = await self.list_available_dimensions(corpus_id=corpus_id)
        if not names:
            return []

        rows = (
            await self._session.execute(
                select(DemographicRow.data)
                .join(DemographicFiles, DemographicRow.demographic_file_id == DemographicFiles.id)
                .where(DemographicFiles.corpus_id == corpus_id)
            )
        ).scalars().all()

        has_value = dict.fromkeys(names, False)
        all_numeric = dict.fromkeys(names, True)
        for data in rows:
            if not data:
                continue
            for name in names:
                raw = data.get(name)
                if raw is None:
                    continue
                text = str(raw).strip()
                if not text:
                    continue
                has_value[name] = True
                if not self._is_numeric_text(text):
                    all_numeric[name] = False

        return [
            DemographicDimensionInfo(name=name, is_numeric=has_value[name] and all_numeric[name])
            for name in names
        ]

    async def get_theme_breakdown(
        self,
        *,
        codebook_id: UUID,
        theme_id: UUID,
        dimensions: list[str],
        application_run_id: UUID | None = None,
        bins: dict[str, int] | None = None,
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

        effective_bins = bins or {}
        return ThemeDemographicBreakdownResponse(
            theme_id=theme_id,
            application_run_id=run_id,
            dimensions=[
                self._build_dimension_breakdown(
                    dimension=dimension,
                    population=population,
                    present_document_ids=present_document_ids,
                    bin_count=effective_bins.get(dimension),
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
        bin_count: int | None = None,
    ) -> ThemeDimensionBreakdown:
        if bin_count is not None:
            return self._build_binned_dimension_breakdown(
                dimension=dimension,
                population=population,
                present_document_ids=present_document_ids,
                bin_count=bin_count,
            )

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

    def _build_binned_dimension_breakdown(
        self,
        *,
        dimension: str,
        population: list[tuple[UUID, dict[str, Any] | None]],
        present_document_ids: set[UUID],
        bin_count: int,
    ) -> ThemeDimensionBreakdown:
        """Group a numeric dimension into equal-width intervals instead of raw values.

        Participants whose value doesn't parse as a number (or is missing)
        still land in the usual NOT_SPECIFIED_LABEL bucket rather than being
        dropped.
        """
        bin_count = max(MIN_BIN_COUNT, min(MAX_BIN_COUNT, bin_count))

        numeric_by_document: dict[UUID, float] = {}
        for document_id, data in population:
            raw = (data or {}).get(dimension)
            if raw is None:
                continue
            text = str(raw).strip()
            if not text or not self._is_numeric_text(text):
                continue
            numeric_by_document[document_id] = float(text)

        if not numeric_by_document:
            # Nothing in this population parses as a number for this
            # dimension (e.g. it's numeric corpus-wide but empty in this
            # run); fall back to categorical grouping so the panel still
            # shows the "Not specified" bucket instead of an empty chart.
            return self._build_dimension_breakdown(
                dimension=dimension,
                population=population,
                present_document_ids=present_document_ids,
            )

        values = list(numeric_by_document.values())
        minimum, maximum = min(values), max(values)
        all_integers = all(value.is_integer() for value in values)
        labels, edges = self._bin_edges_and_labels(
            minimum=minimum, maximum=maximum, bin_count=bin_count, all_integers=all_integers
        )

        group_total: dict[str, int] = defaultdict(int)
        group_present: dict[str, int] = defaultdict(int)
        for document_id, _data in population:
            value = numeric_by_document.get(document_id)
            label = NOT_SPECIFIED_LABEL if value is None else labels[self._bin_index(value, edges)]
            group_total[label] += 1
            if document_id in present_document_ids:
                group_present[label] += 1

        # Show every interval (even empty ones) so the chart always reflects
        # the requested bin count; the catch-all bucket only appears when
        # something actually landed in it.
        ordered_labels = [*labels, *([NOT_SPECIFIED_LABEL] if group_total.get(NOT_SPECIFIED_LABEL) else [])]
        groups = [
            DemographicGroupStat(
                group_value=label,
                present_count=group_present.get(label, 0),
                group_total=group_total.get(label, 0),
                percentage=self._to_percentage(
                    present=group_present.get(label, 0),
                    total=group_total.get(label, 0),
                ),
                small_sample=0 < group_total.get(label, 0) < self._small_sample_threshold,
            )
            for label in ordered_labels
        ]
        return ThemeDimensionBreakdown(dimension=dimension, groups=groups)

    @staticmethod
    def _bin_edges_and_labels(
        *,
        minimum: float,
        maximum: float,
        bin_count: int,
        all_integers: bool,
    ) -> tuple[list[str], list[float]]:
        """Equal-width bin edges over [minimum, maximum], plus display labels.

        Integer-valued dimensions (the common case: age, years, counts) get
        integer, non-overlapping labels like "19-29"; the bin count is capped
        to the number of distinct integers available so labels never collide.
        Everything else uses one-decimal float labels.
        """
        if minimum == maximum:
            bound = str(int(minimum)) if all_integers else f"{minimum:.1f}"
            return [bound], [minimum, maximum]

        effective_bin_count = bin_count
        if all_integers:
            effective_bin_count = max(1, min(bin_count, int(maximum - minimum)))

        width = (maximum - minimum) / effective_bin_count
        edges = [minimum + (index * width) for index in range(effective_bin_count + 1)]
        edges[-1] = maximum

        if all_integers:
            edges = [round(edge) for edge in edges]
            edges[0] = int(minimum)
            edges[-1] = int(maximum)
            for index in range(1, len(edges)):
                if edges[index] <= edges[index - 1]:
                    edges[index] = edges[index - 1] + 1
            labels = [
                f"{edges[index]}-{edges[index + 1]}"
                if index == effective_bin_count - 1
                else f"{edges[index]}-{edges[index + 1] - 1}"
                for index in range(effective_bin_count)
            ]
            return labels, [float(edge) for edge in edges]

        labels = [f"{edges[index]:.1f}-{edges[index + 1]:.1f}" for index in range(effective_bin_count)]
        return labels, edges

    @staticmethod
    def _bin_index(value: float, edges: list[float]) -> int:
        bin_count = len(edges) - 1
        for index in range(bin_count - 1):
            if value < edges[index + 1]:
                return index
        return bin_count - 1

    @staticmethod
    def _is_numeric_text(text: str) -> bool:
        try:
            float(text)
        except ValueError:
            return False
        return True

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
