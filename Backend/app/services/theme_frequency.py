from __future__ import annotations

from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Codebook, CodebookThemeRelationship, Theme
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
    ) -> list[ThemeFrequencyItem]:
        await self._ensure_codebook_exists(codebook_id=codebook_id)
        themes = await self._load_active_themes(codebook_id=codebook_id)
        occurrence_count_by_theme_id = await self._load_occurrence_count_by_theme_id(
            codebook_id=codebook_id,
            theme_ids={theme.id for theme in themes},
        )
        total_interviews_in_corpus = await self._load_total_interviews_in_corpus(
            codebook_id=codebook_id
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
            .where(Theme.is_active.is_(True))
            .distinct()
        )
        return list((await self._session.scalars(stmt)).all())

    async def _load_occurrence_count_by_theme_id(
        self,
        *,
        codebook_id: UUID,
        theme_ids: set[UUID],
    ) -> dict[UUID, int]:
        # TODO: Replace hardcoded zero counts when interview/theme-occurrence
        # persistence exists in the schema.
        del codebook_id
        return {theme_id: 0 for theme_id in theme_ids}

    async def _load_total_interviews_in_corpus(self, *, codebook_id: UUID) -> int:
        # TODO: Replace hardcoded total interview count when codebooks can be
        # linked to persisted interview/corpus entities.
        del codebook_id
        return 0

    @staticmethod
    def _to_coverage_percentage(*, occurrence_count: int, total_interviews: int) -> float:
        if total_interviews <= 0:
            return 0.0
        return (occurrence_count / total_interviews) * 100.0
