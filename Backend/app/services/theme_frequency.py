from __future__ import annotations

from uuid import UUID

from sqlalchemy import and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Codebook,
    CodebookApplicationRun,
    CodebookThemeRelationship,
    DocumentCoding,
    Theme,
    ThemeAssignment,
)
from app.schemas.theme_views import ThemeFrequencyItem
from app.services.theme_graph import ThemeNotFoundError


class ThemeFrequencyService:
    """Build a flat, codebook-scoped theme frequency list."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_theme_frequencies(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None = None,
    ) -> list[ThemeFrequencyItem]:
        await self._ensure_codebook_exists(codebook_id=codebook_id)
        selected_run_id = await self._resolve_application_run_id(
            codebook_id=codebook_id,
            application_run_id=application_run_id,
        )
        themes = await self._load_active_themes(codebook_id=codebook_id)
        occurrence_count_by_theme_id = await self._load_occurrence_count_by_theme_id(
            codebook_id=codebook_id,
            application_run_id=selected_run_id,
            theme_ids={theme.id for theme in themes},
        )
        total_interviews_in_corpus = await self._load_total_interviews_in_corpus(
            codebook_id=codebook_id,
            application_run_id=selected_run_id,
        )

        payload = [
            ThemeFrequencyItem(
                theme_id=theme.id,
                theme_name=theme.label,
                occurrence_count=occurrence_count_by_theme_id.get(theme.id, 0),
                interview_coverage_percentage=self._to_coverage_percentage(
                    occurrence_count=occurrence_count_by_theme_id.get(theme.id, 0),
                    total_interviews=total_interviews_in_corpus,
                ),
            )
            for theme in themes
        ]
        payload.sort(
            key=lambda item: (
                -item.occurrence_count,
                item.theme_name.lower(),
            )
        )
        return payload

    async def _ensure_codebook_exists(self, *, codebook_id: UUID) -> None:
        stmt = select(Codebook.id).where(Codebook.id == codebook_id)
        codebook_row = (await self._session.execute(stmt)).scalar_one_or_none()
        if codebook_row is None:
            raise ThemeNotFoundError(f"Codebook '{codebook_id}' not found.")

    async def _resolve_application_run_id(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None,
    ) -> UUID | None:
        if application_run_id is not None:
            run = await self._session.get(CodebookApplicationRun, application_run_id)
            if run is None or run.codebook_id != codebook_id:
                raise ThemeNotFoundError(
                    f"Codebook application run '{application_run_id}' not found for codebook '{codebook_id}'."
                )
            return run.id

        stmt = (
            select(CodebookApplicationRun.id)
            .where(
                CodebookApplicationRun.codebook_id == codebook_id,
                CodebookApplicationRun.status == "succeeded",
            )
            .order_by(desc(CodebookApplicationRun.finished_at), desc(CodebookApplicationRun.created_at))
            .limit(1)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def _load_active_themes(self, *, codebook_id: UUID) -> list[Theme]:
        stmt = (
            select(Theme)
            .join(
                CodebookThemeRelationship,
                and_(
                    CodebookThemeRelationship.theme_id == Theme.id,
                    CodebookThemeRelationship.codebook_id == codebook_id,
                    CodebookThemeRelationship.is_active.is_(True),
                ),
            )
            .where(
                Theme.is_active.is_(True),
                Theme.codebook_id == codebook_id,
            )
            .distinct()
        )
        return list((await self._session.scalars(stmt)).all())

    async def _load_occurrence_count_by_theme_id(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None,
        theme_ids: set[UUID],
    ) -> dict[UUID, int]:
        del codebook_id
        if application_run_id is None or not theme_ids:
            return {theme_id: 0 for theme_id in theme_ids}
        stmt = (
            select(
                ThemeAssignment.theme_id,
                func.count(func.distinct(DocumentCoding.document_id)),
            )
            .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
            .where(
                DocumentCoding.application_run_id == application_run_id,
                ThemeAssignment.theme_id.in_(theme_ids),
                ThemeAssignment.is_present.is_(True),
            )
            .group_by(ThemeAssignment.theme_id)
        )
        rows = (await self._session.execute(stmt)).all()
        counts = {theme_id: 0 for theme_id in theme_ids}
        counts.update({theme_id: int(count) for theme_id, count in rows})
        return counts

    async def _load_total_interviews_in_corpus(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None,
    ) -> int:
        del codebook_id
        if application_run_id is None:
            return 0
        run = await self._session.get(CodebookApplicationRun, application_run_id)
        if run is None:
            return 0
        return run.documents_total

    @staticmethod
    def _to_coverage_percentage(*, occurrence_count: int, total_interviews: int) -> float:
        if total_interviews <= 0:
            return 0.0
        return (occurrence_count / total_interviews) * 100.0
