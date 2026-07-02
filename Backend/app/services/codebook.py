"""Business logic for creating and reading codebooks with their themes."""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError, UnprocessableError
from app.models.code import Code, CodebookCodeRelationship, ThemeCodeRelationship
from app.models.codebook import Codebook
from app.models.ingestion import Corpus
from app.models.themes import CodebookThemeRelationship, Theme, ThemeHierarchyRelationship
from app.schemas.codebook import (
    MAX_THEMES,
    MIN_THEMES,
    CodebookCreateRequest,
    CodeInCodebookSchema,
    NodeType,
    ThemeInCodebookSchema,
)
from app.services.analysis_dependency_guard import guard_codebook_deletion

if TYPE_CHECKING:
    from app.schemas.codebook import CodebookDetailSchema

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
    ) -> tuple[Codebook, list[Theme], list[ThemeHierarchyRelationship], list[Code], list[ThemeCodeRelationship]]:
        """Persist a new codebook and all its themes and codes atomically.

        Version is auto-incremented per corpus_id (starts at 1).
        Rolls back and raises UnprocessableError on any failure.

        Returns:
            (codebook, themes, edges, codes) — refreshed ORM objects.
        """
        if not (MIN_THEMES <= len(payload.nodes) <= MAX_THEMES):
            raise UnprocessableError(
                f"Codebook must have between {MIN_THEMES} and {MAX_THEMES} nodes; "
                f"got {len(payload.nodes)}."
            )

        try:
            # Verify corpus exists
            corpus_exists = await self._session.execute(
                select(Corpus.id).where(Corpus.id == payload.corpus_id)
            )
            if not corpus_exists.scalar_one_or_none():
                raise UnprocessableError(f"Corpus '{payload.corpus_id}' not found.")

            # Auto-versioning: find the current max version for this project.
            version_q = select(func.max(Codebook.version)).where(
                Codebook.corpus_id == payload.corpus_id
            )
            current_max: int | None = (
                await self._session.execute(version_q)
            ).scalar_one_or_none()
            next_version = (current_max or 0) + 1

            # Insert the codebook.
            codebook = Codebook(
                corpus_id=payload.corpus_id,
                name=payload.name,
                description=None,
                version=next_version,
                created_by=self._DEFAULT_CREATED_BY,
            )
            self._session.add(codebook)
            # Flush so we have codebook.id before inserting related rows.
            await self._session.flush()

            # Insert each theme and code + membership link.
            themes: list[Theme] = []
            codes: list[Code] = []
            edges: list[ThemeHierarchyRelationship] = []
            tc_edges: list[ThemeCodeRelationship] = []
            theme_by_name: dict[str, Theme] = {}
            seen_names: set[str] = set()

            for node_input in payload.nodes:
                if node_input.name in seen_names:
                    raise UnprocessableError(f"Duplicate node name found: '{node_input.name}'")
                seen_names.add(node_input.name)
                if node_input.node_type == NodeType.CODE:
                    code = Code(
                        codebook_id=codebook.id,
                        label=node_input.name,
                        description=node_input.description,
                        is_active=True,
                    )
                    self._session.add(code)
                    codes.append(code)
                else:
                    theme = Theme(
                        codebook_id=codebook.id,
                        label=node_input.name,
                        description=node_input.description,
                        is_active=True,
                    )
                    self._session.add(theme)
                    themes.append(theme)
                    theme_by_name[node_input.name] = theme

            await self._session.flush()  # get theme.id and code.id

            for node_input, code in zip([n for n in payload.nodes if n.node_type == NodeType.CODE], codes, strict=True):
                link = CodebookCodeRelationship(
                    codebook_id=codebook.id,
                    code_id=code.id,
                    is_active=True,
                )
                self._session.add(link)

                if node_input.parent_name:
                    parent_theme = theme_by_name.get(node_input.parent_name)
                    if parent_theme:
                        tc_link = ThemeCodeRelationship(
                            codebook_id=codebook.id,
                            theme_id=parent_theme.id,
                            code_id=code.id,
                            is_active=True,
                        )
                        self._session.add(tc_link)
                        tc_edges.append(tc_link)

            for node_input in payload.nodes:
                if node_input.node_type == NodeType.CODE:
                    continue

                theme = theme_by_name[node_input.name]
                theme_link = CodebookThemeRelationship(
                    codebook_id=codebook.id,
                    theme_id=theme.id,
                    is_active=True,
                )
                self._session.add(theme_link)

                if node_input.parent_name:
                    parent_theme = theme_by_name.get(node_input.parent_name)
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
            for code in codes:
                await self._session.refresh(code)

        except UnprocessableError:
            await self._session.rollback()
            raise
        except Exception as exc:
            await self._session.rollback()
            raise UnprocessableError(
                f"Failed to create codebook: {exc}"
            ) from exc

        return codebook, themes, edges, codes, tc_edges

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get_codebook_detail(
        self, codebook_id: uuid.UUID
    ) -> tuple[Codebook, list[Theme], list[ThemeHierarchyRelationship], list[Code], list[ThemeCodeRelationship]]:
        """Fetch a codebook, its themes, and its codes.

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

        # Fetch ThemeCode edges
        theme_code_edges_result = await self._session.execute(
            select(ThemeCodeRelationship)
            .where(
                ThemeCodeRelationship.codebook_id == codebook_id,
                ThemeCodeRelationship.is_active.is_(True),
            )
        )
        theme_code_edges = list(theme_code_edges_result.scalars().all())

        # Fetch codes
        codes_result = await self._session.execute(
            select(Code)
            .join(
                CodebookCodeRelationship,
                CodebookCodeRelationship.code_id == Code.id,
            )
            .where(
                CodebookCodeRelationship.codebook_id == codebook_id,
                CodebookCodeRelationship.is_active.is_(True),
                Code.is_active.is_(True),
            )
            .order_by(Code.label)
        )
        codes = list(codes_result.scalars().all())

        return codebook, themes, edges, codes, theme_code_edges

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    async def delete_codebook(self, codebook_id: uuid.UUID, *, force: bool = False) -> None:
        """Delete a codebook and all associated themes/codes via cascade.

        Raises:
            NotFoundError: If no codebook with ``codebook_id`` exists.
        """
        codebook_result = await self._session.execute(
            select(Codebook).where(Codebook.id == codebook_id)
        )
        codebook = codebook_result.scalar_one_or_none()
        if codebook is None:
            raise NotFoundError(f"Codebook '{codebook_id}' not found.")

        await guard_codebook_deletion(
            self._session,
            codebook_ids=[codebook_id],
            force=force,
        )
        await self._session.delete(codebook)
        await self._session.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def build_detail_schema(
        codebook: Codebook,
        themes: list[Theme],
        edges: list[ThemeHierarchyRelationship] | None = None,
        codes: list[Code] | None = None,
        theme_code_edges: list[ThemeCodeRelationship] | None = None
    ) -> CodebookDetailSchema:
        """Build a CodebookDetailSchema from ORM objects.

        Imported inline to avoid circular imports at module level.
        """
        from app.schemas.codebook import CodebookDetailSchema

        edges = edges or []
        codes = codes or []

        # Roots are those without any parent edge pointing to them
        child_ids = {edge.child_theme_id for edge in edges}

        schema_by_id = {
            t.id: ThemeInCodebookSchema.from_theme(t, is_subtheme=(t.id in child_ids)) for t in themes
        }

        # Build tree: assign children to parents
        for edge in edges:
            parent = schema_by_id.get(edge.parent_theme_id)
            child = schema_by_id.get(edge.child_theme_id)
            if parent and child:
                parent.children.append(child)

        code_schemas = [CodeInCodebookSchema.from_code(c) for c in codes]
        code_schema_by_id = {c.id: c for c in code_schemas}

        theme_code_edges = theme_code_edges or []
        for tc_edge in theme_code_edges:
            parent = schema_by_id.get(tc_edge.theme_id)
            child_code = code_schema_by_id.get(tc_edge.code_id)
            if parent and child_code:
                parent.children.append(child_code)

        root_themes = [
            schema for t_id, schema in schema_by_id.items() if t_id not in child_ids
        ]

        code_schemas = [CodeInCodebookSchema.from_code(c) for c in codes]

        return CodebookDetailSchema(
            id=codebook.id,
            corpus_id=codebook.corpus_id,
            name=codebook.name,
            description=codebook.description,
            version=codebook.version,
            created_by=codebook.created_by,
            research_query=codebook.research_query,
            researcher_topics=codebook.researcher_topics,
            llm_tokens_input=codebook.llm_tokens_input,
            llm_tokens_output=codebook.llm_tokens_output,
            themes=root_themes,
            codes=code_schemas,
        )
