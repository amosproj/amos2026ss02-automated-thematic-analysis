from __future__ import annotations

from collections import defaultdict
from uuid import UUID

from anytree import AnyNode
from anytree.node.exceptions import LoopError
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Code,
    Codebook,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Theme,
    ThemeCodeRelationship,
    ThemeHierarchyRelationship,
)
from app.schemas.theme_graph import (
    ThemeDagValidation,
    ThemeDagView,
    ThemeEdgeView,
    ThemeNodeView,
    ThemeTreeNode,
)


class ThemeGraphError(Exception):
    """Base exception for theme tree operations."""


class ThemeNotFoundError(ThemeGraphError):
    """Raised when a codebook or theme does not exist in the active scope."""


class ThemeValidationError(ThemeGraphError):
    """Raised when hierarchy data cannot form a valid tree."""


class ThemeGraphService:
    """DB-backed theme tree builder using minimal relationship tables."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def validate_theme_dag(self, *, codebook_id: UUID) -> ThemeDagValidation:
        """Validate active membership + hierarchy edges for tree constraints."""
        await self._ensure_codebook_exists(codebook_id)
        nodes = await self._load_theme_nodes(codebook_id=codebook_id)
        edges = await self._load_hierarchy_edges(codebook_id=codebook_id, theme_ids=set(nodes))

        violations: list[str] = []
        parents_by_child: defaultdict[UUID, set[UUID]] = defaultdict(set)
        for edge in edges:
            # Collect distinct parents for each child to enforce tree semantics.
            parents_by_child[edge.child_theme_id].add(edge.parent_theme_id)
            if edge.child_theme_id == edge.parent_theme_id:
                violations.append(f"Theme '{edge.child_theme_id}' cannot be parent of itself.")

        multi_parent_children = sorted(
            (
                child_id
                for child_id, parent_ids in parents_by_child.items()
                if len(parent_ids) > 1
            ),
            key=str,
        )
        if multi_parent_children:
            joined = ", ".join(str(theme_id) for theme_id in multi_parent_children)
            violations.append(f"Themes with multiple active parents: {joined}")

        try:
            # `anytree` raises a LoopError when parent assignments form a cycle.
            self._materialize_anytree(nodes=nodes, edges=edges)
        except ThemeValidationError as exc:
            violations.append(str(exc))

        return ThemeDagValidation(is_valid=not violations, violations=tuple(violations))

    async def build_theme_dag(self, *, codebook_id: UUID) -> ThemeDagView:
        """
        Build a DAG-like view (nodes, hierarchy edges, roots) from active DB rows.

        The hierarchy is constrained to tree semantics (single active parent per child).
        """
        await self._ensure_codebook_exists(codebook_id)
        nodes = await self._load_theme_nodes(codebook_id=codebook_id)
        edges = await self._load_hierarchy_edges(codebook_id=codebook_id, theme_ids=set(nodes))
        runtime_nodes = self._materialize_anytree(nodes=nodes, edges=edges)

        # Roots are nodes without a parent after applying all active edges.
        root_theme_ids = sorted(
            (theme_id for theme_id, node in runtime_nodes.items() if node.parent is None),
            key=lambda theme_id: nodes[theme_id].label.lower(),
        )
        edge_views = [
            ThemeEdgeView(child_theme_id=edge.child_theme_id, parent_theme_id=edge.parent_theme_id)
            for edge in edges
        ]
        edge_views.sort(key=lambda edge: (str(edge.parent_theme_id), str(edge.child_theme_id)))

        return ThemeDagView(
            codebook_id=codebook_id,
            nodes=nodes,
            edges=edge_views,
            root_theme_ids=root_theme_ids,
        )

    async def get_theme_tree(
        self,
        *,
        codebook_id: UUID,
        root_theme_id: UUID | None = None,
    ) -> list[ThemeTreeNode]:
        """Build a nested, unbalanced tree from active DB rows using anytree."""
        dag = await self.build_theme_dag(codebook_id=codebook_id)
        # Rehydrate runtime anytree nodes from the immutable schema payload.
        runtime_nodes = self._materialize_anytree(nodes=dag.nodes, edges=dag.edges)

        if root_theme_id is not None:
            if root_theme_id not in runtime_nodes:
                raise ThemeNotFoundError(
                    f"Theme '{root_theme_id}' not found in codebook '{codebook_id}'."
                )
            roots = [runtime_nodes[root_theme_id]]
        else:
            roots = [runtime_nodes[root_id] for root_id in dag.root_theme_ids]

        return [self._to_tree_node(node) for node in roots]

    async def _ensure_codebook_exists(self, codebook_id: UUID) -> None:
        # Validate codebook identity early so downstream errors are specific.
        stmt = select(Codebook.id).where(Codebook.id == codebook_id)
        codebook_row = (await self._session.execute(stmt)).scalar_one_or_none()
        if codebook_row is None:
            raise ThemeNotFoundError(f"Codebook '{codebook_id}' not found.")

    async def _load_theme_nodes(self, *, codebook_id: UUID) -> dict[UUID, ThemeNodeView]:
        # Restrict to active themes that are active members of the target codebook.
        stmt_themes = (
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
        )
        themes = list((await self._session.scalars(stmt_themes)).all())

        stmt_codes = (
            select(Code)
            .join(
                CodebookCodeRelationship,
                and_(
                    CodebookCodeRelationship.code_id == Code.id,
                    CodebookCodeRelationship.codebook_id == codebook_id,
                    CodebookCodeRelationship.is_active.is_(True),
                ),
            )
            .where(
                Code.is_active.is_(True),
                Code.codebook_id == codebook_id,
            )
        )
        codes = list((await self._session.scalars(stmt_codes)).all())

        nodes = {}
        for theme in themes:
            nodes[theme.id] = ThemeNodeView(
                id=theme.id,
                label=theme.label,
                is_active=theme.is_active,
                node_type="THEME"
            )
        for code in codes:
            nodes[code.id] = ThemeNodeView(
                id=code.id,
                label=code.label,
                is_active=code.is_active,
                node_type="CODE"
            )
        return nodes

    async def _load_hierarchy_edges(
        self,
        *,
        codebook_id: UUID,
        theme_ids: set[UUID],
    ) -> list[ThemeEdgeView]:
        if not theme_ids:
            return []
        # Ignore dangling edges that reference nodes outside the active membership set.
        stmt = select(ThemeHierarchyRelationship).where(
            ThemeHierarchyRelationship.codebook_id == codebook_id,
            ThemeHierarchyRelationship.is_active.is_(True),
            ThemeHierarchyRelationship.parent_theme_id.in_(theme_ids),
            ThemeHierarchyRelationship.child_theme_id.in_(theme_ids),
        )
        theme_edges = list((await self._session.scalars(stmt)).all())

        stmt_code = select(ThemeCodeRelationship).where(
            ThemeCodeRelationship.codebook_id == codebook_id,
            ThemeCodeRelationship.is_active.is_(True),
            ThemeCodeRelationship.theme_id.in_(theme_ids),
            ThemeCodeRelationship.code_id.in_(theme_ids),
        )
        code_edges = list((await self._session.scalars(stmt_code)).all())

        edges = [
            ThemeEdgeView(parent_theme_id=e.parent_theme_id, child_theme_id=e.child_theme_id)
            for e in theme_edges
        ]
        edges.extend([
            ThemeEdgeView(parent_theme_id=e.theme_id, child_theme_id=e.code_id)
            for e in code_edges
        ])
        return edges

    def _materialize_anytree(
        self,
        *,
        nodes: dict[UUID, ThemeNodeView],
        edges: list[ThemeHierarchyRelationship] | list[ThemeEdgeView],
    ) -> dict[UUID, AnyNode]:
        # Keep transport schemas immutable; build a dedicated runtime tree structure.
        runtime_nodes: dict[UUID, AnyNode] = {
            theme_id: AnyNode(theme=theme_view)
            for theme_id, theme_view in nodes.items()
        }
        assigned_parent_for_child: dict[UUID, UUID] = {}

        for edge in edges:
            child_id = edge.child_theme_id
            parent_id = edge.parent_theme_id
            if child_id not in runtime_nodes or parent_id not in runtime_nodes:
                # Defensive guard for stale edges; query filters should already prevent this.
                continue
            if child_id in assigned_parent_for_child and assigned_parent_for_child[child_id] != parent_id:
                raise ThemeValidationError(f"Theme '{child_id}' has multiple active parents.")
            assigned_parent_for_child[child_id] = parent_id
            try:
                # Parent assignment is where anytree enforces acyclic structure.
                runtime_nodes[child_id].parent = runtime_nodes[parent_id]
            except LoopError as exc:
                raise ThemeValidationError("Hierarchy contains a cycle.") from exc

        return runtime_nodes

    def _to_tree_node(self, node: AnyNode) -> ThemeTreeNode:
        # Sort children for deterministic API responses and stable test output.
        sorted_children = sorted(
            node.children,
            key=lambda child: child.theme.label.lower(),
        )
        return ThemeTreeNode(
            theme=node.theme,
            children=[self._to_tree_node(child) for child in sorted_children],
        )
