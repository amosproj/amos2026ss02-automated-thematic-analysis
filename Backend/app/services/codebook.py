"""Business logic for creating and reading codebooks with their themes."""
from __future__ import annotations

import uuid

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError, UnprocessableError
from app.models.codebook import Codebook
from app.models.themes import CodebookThemeRelationship, Theme, ThemeHierarchyRelationship
from app.schemas.codebook import (
    MAX_THEMES,
    MIN_THEMES,
    CodebookCreateRequest,
    ThemeInCodebookSchema,
)


class CodebookService:
    """All database operations for Codebook, Theme, and CodebookThemeRelationship."""

    # Placeholder used until authentication is implemented.
    _DEFAULT_CREATED_BY = "researcher"

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    async def create_codebook(
        self, payload: CodebookCreateRequest
    ) -> tuple[Codebook, list[Theme], list[ThemeHierarchyRelationship]]:
        """Persist a new codebook and all its themes atomically.

        Version is auto-incremented per project_id (starts at 1).
        Rolls back and raises UnprocessableError on any failure.

        Returns:
            (codebook, themes) — refreshed ORM objects.
        """
        if not (MIN_THEMES <= len(payload.themes) <= MAX_THEMES):
            raise UnprocessableError(
                f"Codebook must have between {MIN_THEMES} and {MAX_THEMES} themes; "
                f"got {len(payload.themes)}."
            )

        try:
            # Auto-versioning: find the current max version for this project.
            version_q = select(func.max(Codebook.version)).where(
                Codebook.project_id == payload.project_id
            )
            current_max: int | None = (
                await self._session.execute(version_q)
            ).scalar_one_or_none()
            next_version = (current_max or 0) + 1

            # Insert the codebook.
            codebook = Codebook(
                project_id=payload.project_id,
                name=payload.name,
                description=None,
                version=next_version,
                created_by=self._DEFAULT_CREATED_BY,
            )
            self._session.add(codebook)
            # Flush so we have codebook.id before inserting related rows.
            await self._session.flush()

            # Insert each theme + membership link.
            themes: list[Theme] = []
            edges: list[ThemeHierarchyRelationship] = []
            theme_by_name: dict[str, Theme] = {}

            for theme_input in payload.themes:
                theme = Theme(
                    node_type=theme_input.node_type,
                    label=theme_input.name,
                    description=theme_input.description,
                    is_active=True,
                )
                self._session.add(theme)
                themes.append(theme)
                theme_by_name[theme_input.name] = theme

            await self._session.flush()  # get theme.id for all themes

            for theme_input in payload.themes:
                theme = theme_by_name[theme_input.name]
                link = CodebookThemeRelationship(
                    codebook_id=codebook.id,
                    theme_id=theme.id,
                    is_active=True,
                )
                self._session.add(link)

                if theme_input.parent_name:
                    parent_theme = theme_by_name.get(theme_input.parent_name)
                    if parent_theme:
                        hierarchy_link = ThemeHierarchyRelationship(
                            codebook_id=codebook.id,
                            parent_theme_id=parent_theme.id,
                            child_theme_id=theme.id,
                            is_active=True,
                        )
                        self._session.add(hierarchy_link)
                        edges.append(hierarchy_link)

            await self._session.commit()
            await self._session.refresh(codebook)
            for theme in themes:
                await self._session.refresh(theme)

        except UnprocessableError:
            await self._session.rollback()
            raise
        except Exception as exc:
            await self._session.rollback()
            raise UnprocessableError(
                f"Failed to create codebook: {exc}"
            ) from exc

        return codebook, themes, edges

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get_codebook_detail(
        self, codebook_id: uuid.UUID
    ) -> tuple[Codebook, list[Theme], list[ThemeHierarchyRelationship]]:
        """Fetch a codebook and its active themes.

        Raises:
            NotFoundError: If no codebook with ``codebook_id`` exists.
        """
        codebook_result = await self._session.execute(
            select(Codebook).where(Codebook.id == codebook_id)
        )
        codebook = codebook_result.scalar_one_or_none()
        if codebook is None:
            raise NotFoundError(f"Codebook '{codebook_id}' not found.")

        themes_result = await self._session.execute(
            select(Theme)
            .join(
                CodebookThemeRelationship,
                CodebookThemeRelationship.theme_id == Theme.id,
            )
            .where(
                CodebookThemeRelationship.codebook_id == codebook_id,
                CodebookThemeRelationship.is_active.is_(True),
                Theme.is_active.is_(True),
            )
            .order_by(Theme.label)
        )
        themes = list(themes_result.scalars().all())

        # Also fetch the hierarchy edges for this codebook
        edges_result = await self._session.execute(
            select(ThemeHierarchyRelationship)
            .where(
                ThemeHierarchyRelationship.codebook_id == codebook_id,
                ThemeHierarchyRelationship.is_active.is_(True),
            )
        )
        edges = list(edges_result.scalars().all())

        return codebook, themes, edges

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def build_detail_schema(
        codebook: Codebook, themes: list[Theme], edges: list[ThemeHierarchyRelationship] | None = None
    ):
        """Build a CodebookDetailSchema from ORM objects.

        Imported inline to avoid circular imports at module level.
        """
        from app.schemas.codebook import CodebookDetailSchema, ThemeInCodebookSchema

        edges = edges or []
        schema_by_id = {
            t.id: ThemeInCodebookSchema.from_theme(t) for t in themes
        }

        # Build tree: assign children to parents
        for edge in edges:
            parent = schema_by_id.get(edge.parent_theme_id)
            child = schema_by_id.get(edge.child_theme_id)
            if parent and child:
                parent.children.append(child)

        # Roots are those without any parent edge pointing to them
        child_ids = {edge.child_theme_id for edge in edges}
        root_themes = [
            schema for t_id, schema in schema_by_id.items() if t_id not in child_ids
        ]

        return CodebookDetailSchema(
            id=codebook.id,
            project_id=codebook.project_id,
            name=codebook.name,
            description=codebook.description,
            version=codebook.version,
            created_by=codebook.created_by,
            themes=root_themes,
        )
