from __future__ import annotations

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.theme_views import ThemeTreeResponse
from app.services.theme_graph import ThemeGraphService


class ThemeReadService:
    """Read-side wrapper for theme tree lookups."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_theme_tree(
        self,
        *,
        codebook_id: UUID,
        root_theme_id: UUID | None = None,
    ) -> ThemeTreeResponse:
        tree = await ThemeGraphService(self._session).get_theme_tree(
            codebook_id=codebook_id,
            root_theme_id=root_theme_id,
        )
        return ThemeTreeResponse(
            codebook_id=codebook_id,
            root_theme_id=root_theme_id,
            tree=tree,
        )
