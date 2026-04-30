from __future__ import annotations

"""
Theme graph service for thematic-analysis codebook evolution.

This module focuses on theme-level operations only. The service models the
theme space as a DAG where:

1. `CHILD_OF` edges capture hierarchy (subtheme -> parent theme).
2. `RELATED_TO` and `EQUIVALENT_TO` capture cross-links in the same codebook.
3. Node and edge statuses preserve lineage over iterative refinement rounds.

The operation set mirrors the refinement actions discussed in the two papers:
add, move, merge, split, replace, and retire nodes while keeping provenance.
"""

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Iterable
from uuid import UUID, uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import (
    ActorType,
    CodebookThemeRelationshipType,
    NodeStatus,
    RelationshipStatus,
    ThemeLevel,
    ThemeRelationshipType,
)
from app.models import Codebook, CodebookThemeRelationship, Theme, ThemeRelationship
from app.schemas.theme_graph import (
    ThemeDagValidation,
    ThemeDagView,
    ThemeEdgeView,
    ThemeNodeView,
    ThemeTreeNode,
)


DEFAULT_WORKING_THEME_STATUSES: frozenset[NodeStatus] = frozenset(
    {NodeStatus.CANDIDATE, NodeStatus.ACTIVE}
)


class ThemeGraphError(Exception):
    """Base exception for theme graph operations."""


class ThemeNotFoundError(ThemeGraphError):
    """Raised when a codebook/theme node cannot be resolved."""


class ThemeConflictError(ThemeGraphError):
    """Raised when an operation conflicts with current graph state."""


class ThemeValidationError(ThemeGraphError):
    """Raised when an operation would violate graph invariants."""


@dataclass(slots=True, frozen=True)
class NewThemeSpec:
    """Input payload for creating a new theme inside split/replace operations."""

    label: str
    description: str
    level: ThemeLevel | None = None
    status: NodeStatus = NodeStatus.CANDIDATE


class ThemeGraphService:
    """
    Service-only theme graph API.

    Notes:
    - Hierarchy orientation uses source=child, target=parent for CHILD_OF edges.
    - Non-hierarchical edges use RELATED_TO / EQUIVALENT_TO.
    - No FastAPI/router dependency; this class works directly on AsyncSession.
    """

    def __init__(self, session: AsyncSession, *, auto_commit: bool = True) -> None:
        self._session = session
        self._auto_commit = auto_commit

    async def create_theme(
        self,
        *,
        codebook_id: UUID,
        label: str,
        description: str,
        level: ThemeLevel,
        created_by: ActorType,
        status: NodeStatus = NodeStatus.CANDIDATE,
        parent_theme_id: UUID | None = None,
        provenance: str | None = None,
    ) -> Theme:
        """
        Create a theme and attach it to the target codebook.

        If `parent_theme_id` is provided, the method also creates a `CHILD_OF`
        edge from the new node to that parent.
        """
        await self._ensure_codebook_exists(codebook_id)
        theme = self._create_theme_row(
            label=label,
            description=description,
            level=level,
            status=status,
            created_by=created_by,
        )
        self._session.add(theme)
        await self._session.flush([theme])
        self._session.add(
            CodebookThemeRelationship(
                id=self._new_id(),
                codebook_id=codebook_id,
                theme_id=theme.id,
                relationship_type=CodebookThemeRelationshipType.CONTAINS,
                status=RelationshipStatus.ACTIVE,
                created_by=created_by,
                provenance=provenance,
            )
        )
        if parent_theme_id is not None:
            await self._ensure_theme_in_codebook(codebook_id, parent_theme_id)
            await self._add_relationship_row(
                codebook_id=codebook_id,
                source_theme_id=theme.id,
                target_theme_id=parent_theme_id,
                relationship_type=ThemeRelationshipType.CHILD_OF,
                created_by=created_by,
                provenance=provenance,
            )
        await self._finalize()
        return theme

    async def update_theme(
        self,
        *,
        codebook_id: UUID,
        theme_id: UUID,
        label: str | None = None,
        description: str | None = None,
        level: ThemeLevel | None = None,
        status: NodeStatus | None = None,
    ) -> Theme:
        """Patch mutable theme fields in-place for a single codebook member."""
        theme = await self._ensure_theme_in_codebook(codebook_id, theme_id)
        if label is not None:
            theme.label = label
        if description is not None:
            theme.description = description
        if level is not None:
            theme.level = level
        if status is not None:
            theme.status = status
        await self._finalize()
        return theme

    async def deprecate_theme(
        self, *, codebook_id: UUID, theme_id: UUID, _reason: str | None = None
    ) -> Theme:
        """Mark a theme as deprecated without removing its historic links."""
        theme = await self._ensure_theme_in_codebook(codebook_id, theme_id)
        theme.status = NodeStatus.DEPRECATED
        await self._finalize()
        return theme

    async def delete_theme(self, *, codebook_id: UUID, theme_id: UUID, hard: bool = False) -> None:
        """
        Remove a theme from active use.

        Soft delete keeps row history and marks connected edges as removed.
        Hard delete physically removes the theme if it is not shared across
        multiple active codebooks.
        """
        theme = await self._ensure_theme_in_codebook(codebook_id, theme_id)
        if hard:
            await self._hard_delete_theme(codebook_id=codebook_id, theme=theme)
            await self._finalize()
            return

        theme.status = NodeStatus.DELETED
        await self._remove_codebook_theme_membership(codebook_id=codebook_id, theme_id=theme_id)
        await self._deactivate_theme_relationships(codebook_id=codebook_id, theme_id=theme_id)
        await self._finalize()

    async def add_child_theme(
        self,
        *,
        codebook_id: UUID,
        parent_theme_id: UUID,
        child_theme_id: UUID,
        created_by: ActorType,
        provenance: str | None = None,
    ) -> ThemeRelationship:
        """Attach an existing child theme to an existing parent theme."""
        await self._ensure_theme_in_codebook(codebook_id, parent_theme_id)
        await self._ensure_theme_in_codebook(codebook_id, child_theme_id)
        relationship = await self._add_relationship_row(
            codebook_id=codebook_id,
            source_theme_id=child_theme_id,
            target_theme_id=parent_theme_id,
            relationship_type=ThemeRelationshipType.CHILD_OF,
            created_by=created_by,
            provenance=provenance,
        )
        await self._finalize()
        return relationship

    async def remove_child_theme(
        self,
        *,
        codebook_id: UUID,
        parent_theme_id: UUID,
        child_theme_id: UUID,
    ) -> int:
        """Deactivate matching active `CHILD_OF` edges and return how many changed."""
        relationships = await self._get_active_relationships(
            codebook_id=codebook_id,
            source_theme_id=child_theme_id,
            target_theme_id=parent_theme_id,
            relationship_type=ThemeRelationshipType.CHILD_OF,
        )
        for relationship in relationships:
            relationship.status = RelationshipStatus.REMOVED
        await self._finalize()
        return len(relationships)

    async def move_theme(
        self,
        *,
        codebook_id: UUID,
        theme_id: UUID,
        new_parent_theme_id: UUID,
        created_by: ActorType,
        provenance: str | None = None,
    ) -> ThemeRelationship:
        """
        Move a theme to a new parent.

        The method enforces a single active parent by removing existing active
        parent links before creating the new one.
        """
        await self._ensure_theme_in_codebook(codebook_id, theme_id)
        await self._ensure_theme_in_codebook(codebook_id, new_parent_theme_id)
        if theme_id == new_parent_theme_id:
            raise ThemeValidationError("A theme cannot be the parent of itself.")

        existing_parents = await self._get_active_relationships(
            codebook_id=codebook_id,
            source_theme_id=theme_id,
            relationship_type=ThemeRelationshipType.CHILD_OF,
        )
        for rel in existing_parents:
            rel.status = RelationshipStatus.REMOVED

        relationship = await self._add_relationship_row(
            codebook_id=codebook_id,
            source_theme_id=theme_id,
            target_theme_id=new_parent_theme_id,
            relationship_type=ThemeRelationshipType.CHILD_OF,
            created_by=created_by,
            provenance=provenance,
        )
        await self._finalize()
        return relationship

    async def add_theme_relation(
        self,
        *,
        codebook_id: UUID,
        source_theme_id: UUID,
        target_theme_id: UUID,
        relationship_type: ThemeRelationshipType,
        created_by: ActorType,
        provenance: str | None = None,
    ) -> ThemeRelationship:
        """Create non-hierarchical semantic links (`RELATED_TO` / `EQUIVALENT_TO`)."""
        if relationship_type == ThemeRelationshipType.CHILD_OF:
            raise ThemeValidationError("Use add_child_theme for CHILD_OF relationships.")

        await self._ensure_theme_in_codebook(codebook_id, source_theme_id)
        await self._ensure_theme_in_codebook(codebook_id, target_theme_id)
        relationship = await self._add_relationship_row(
            codebook_id=codebook_id,
            source_theme_id=source_theme_id,
            target_theme_id=target_theme_id,
            relationship_type=relationship_type,
            created_by=created_by,
            provenance=provenance,
        )
        await self._finalize()
        return relationship

    async def remove_theme_relation(
        self,
        *,
        codebook_id: UUID,
        source_theme_id: UUID,
        target_theme_id: UUID,
        relationship_type: ThemeRelationshipType,
    ) -> int:
        """Deactivate active non-hierarchical relations matching the selector."""
        relationships = await self._get_active_relationships(
            codebook_id=codebook_id,
            source_theme_id=source_theme_id,
            target_theme_id=target_theme_id,
            relationship_type=relationship_type,
        )
        for relationship in relationships:
            relationship.status = RelationshipStatus.REMOVED
        await self._finalize()
        return len(relationships)

    async def merge_themes(
        self,
        *,
        codebook_id: UUID,
        source_theme_ids: Iterable[UUID],
        merged_label: str,
        merged_description: str,
        created_by: ActorType,
        merged_level: ThemeLevel | None = None,
        parent_theme_id: UUID | None = None,
        provenance: str | None = None,
    ) -> Theme:
        """
        Merge multiple source themes into a new consolidated theme.

        Design intent:
        - Keep history by creating a new node and marking source nodes as MERGED.
        - Rewire incoming/outgoing edges from source nodes to the new node.
        - Keep at most one parent for the merged node.

        This follows the refinement pattern from iterative codebook papers:
        merge/synthesize while preserving an auditable lineage.
        """
        source_ids = self._unique_ids(source_theme_ids)
        if len(source_ids) < 2:
            raise ThemeValidationError("merge_themes requires at least two source themes.")

        source_themes = await self._get_themes_in_codebook(codebook_id=codebook_id, theme_ids=source_ids)
        if len(source_themes) != len(source_ids):
            missing = sorted((set(source_ids) - {theme.id for theme in source_themes}), key=str)
            raise ThemeNotFoundError(f"Theme(s) not found in codebook: {', '.join(map(str, missing))}")

        if merged_level is None:
            merged_level = source_themes[0].level
        merged_theme = self._create_theme_row(
            label=merged_label,
            description=merged_description,
            level=merged_level,
            status=NodeStatus.CANDIDATE,
            created_by=created_by,
        )
        self._session.add(merged_theme)
        await self._session.flush([merged_theme])
        self._session.add(
            CodebookThemeRelationship(
                id=self._new_id(),
                codebook_id=codebook_id,
                theme_id=merged_theme.id,
                relationship_type=CodebookThemeRelationshipType.CONTAINS,
                status=RelationshipStatus.ACTIVE,
                created_by=created_by,
                provenance=provenance,
            )
        )

        touched_edges = await self._get_relationships_touching_themes(
            codebook_id=codebook_id,
            theme_ids=source_ids,
        )
        # We keep edge history by deactivating old edges first, then creating
        # rewired replacements below.
        for edge in touched_edges:
            edge.status = RelationshipStatus.REMOVED

        parent_candidates: set[UUID] = set()
        for edge in touched_edges:
            if edge.relationship_type != ThemeRelationshipType.CHILD_OF:
                continue
            if edge.source_theme_id in source_ids and edge.target_theme_id not in source_ids:
                parent_candidates.add(edge.target_theme_id)

        if parent_theme_id is not None:
            await self._ensure_theme_in_codebook(codebook_id, parent_theme_id)
            parent_candidates = {parent_theme_id}
        if len(parent_candidates) > 1:
            options = ", ".join(map(str, sorted(parent_candidates, key=str)))
            raise ThemeValidationError(
                "merge_themes resolved multiple parent candidates for merged node. "
                f"Provide parent_theme_id explicitly. Candidates: {options}"
            )
        if len(parent_candidates) == 1:
            parent_id = next(iter(parent_candidates))
            await self._add_relationship_row(
                codebook_id=codebook_id,
                source_theme_id=merged_theme.id,
                target_theme_id=parent_id,
                relationship_type=ThemeRelationshipType.CHILD_OF,
                created_by=created_by,
                provenance=provenance,
            )

        emitted: set[tuple[UUID, UUID, ThemeRelationshipType]] = set()
        for edge in touched_edges:
            new_source = merged_theme.id if edge.source_theme_id in source_ids else edge.source_theme_id
            new_target = merged_theme.id if edge.target_theme_id in source_ids else edge.target_theme_id
            if new_source == new_target:
                continue
            key = (new_source, new_target, edge.relationship_type)
            if key in emitted:
                continue
            if edge.relationship_type == ThemeRelationshipType.CHILD_OF and new_source == merged_theme.id:
                # Parent handling for merged node is resolved above.
                continue
            emitted.add(key)
            await self._add_relationship_row(
                codebook_id=codebook_id,
                source_theme_id=new_source,
                target_theme_id=new_target,
                relationship_type=edge.relationship_type,
                created_by=created_by,
                provenance=provenance,
            )

        for theme in source_themes:
            theme.status = NodeStatus.MERGED

        await self._finalize()
        return merged_theme

    async def split_theme(
        self,
        *,
        codebook_id: UUID,
        source_theme_id: UUID,
        split_specs: Iterable[NewThemeSpec],
        created_by: ActorType,
        inherit_parent: bool = True,
        provenance: str | None = None,
    ) -> list[Theme]:
        """
        Split one source theme into two or more new themes.

        When `inherit_parent=True`, new themes receive the old parent and the
        old parent edges are removed from the source node.
        """
        source_theme = await self._ensure_theme_in_codebook(codebook_id, source_theme_id)
        specs = list(split_specs)
        if len(specs) < 2:
            raise ThemeValidationError("split_theme requires at least two target themes.")

        parent_edges = await self._get_active_relationships(
            codebook_id=codebook_id,
            source_theme_id=source_theme_id,
            relationship_type=ThemeRelationshipType.CHILD_OF,
        )
        parent_ids = [edge.target_theme_id for edge in parent_edges]
        if inherit_parent and len(set(parent_ids)) > 1:
            options = ", ".join(map(str, sorted(set(parent_ids), key=str)))
            raise ThemeValidationError(
                "Cannot split with inherit_parent when source has multiple parents. "
                f"Parents: {options}"
            )

        created_themes: list[Theme] = []
        for spec in specs:
            level = spec.level if spec.level is not None else source_theme.level
            theme = self._create_theme_row(
                label=spec.label,
                description=spec.description,
                level=level,
                status=spec.status,
                created_by=created_by,
            )
            created_themes.append(theme)
            self._session.add(theme)
            await self._session.flush([theme])
            self._session.add(
                CodebookThemeRelationship(
                    id=self._new_id(),
                    codebook_id=codebook_id,
                    theme_id=theme.id,
                    relationship_type=CodebookThemeRelationshipType.CONTAINS,
                    status=RelationshipStatus.ACTIVE,
                    created_by=created_by,
                    provenance=provenance,
                )
            )

            if inherit_parent and parent_ids:
                await self._add_relationship_row(
                    codebook_id=codebook_id,
                    source_theme_id=theme.id,
                    target_theme_id=parent_ids[0],
                    relationship_type=ThemeRelationshipType.CHILD_OF,
                    created_by=created_by,
                    provenance=provenance,
                )

        if inherit_parent:
            for edge in parent_edges:
                edge.status = RelationshipStatus.REMOVED
        # Keep source as historical anchor in the lineage trail.
        source_theme.status = NodeStatus.DEPRECATED

        await self._finalize()
        return created_themes

    async def replace_theme(
        self,
        *,
        codebook_id: UUID,
        old_theme_id: UUID,
        new_theme_spec: NewThemeSpec,
        created_by: ActorType,
        new_parent_theme_id: UUID | None = None,
        provenance: str | None = None,
    ) -> Theme:
        """
        Replace one theme with a newly created theme.

        Unlike merge/split, replacement is a one-to-one swap that rewires links
        from the old node to the new node and deprecates the old node.
        """
        old_theme = await self._ensure_theme_in_codebook(codebook_id, old_theme_id)
        new_level = new_theme_spec.level if new_theme_spec.level is not None else old_theme.level

        new_theme = self._create_theme_row(
            label=new_theme_spec.label,
            description=new_theme_spec.description,
            level=new_level,
            status=new_theme_spec.status,
            created_by=created_by,
        )
        self._session.add(new_theme)
        await self._session.flush([new_theme])
        self._session.add(
            CodebookThemeRelationship(
                id=self._new_id(),
                codebook_id=codebook_id,
                theme_id=new_theme.id,
                relationship_type=CodebookThemeRelationshipType.CONTAINS,
                status=RelationshipStatus.ACTIVE,
                created_by=created_by,
                provenance=provenance,
            )
        )

        touched_edges = await self._get_relationships_touching_themes(
            codebook_id=codebook_id,
            theme_ids=[old_theme_id],
        )
        # Deactivate first, then emit rewired edges to avoid unique-index clashes.
        for edge in touched_edges:
            edge.status = RelationshipStatus.REMOVED

        emitted: set[tuple[UUID, UUID, ThemeRelationshipType]] = set()
        for edge in touched_edges:
            new_source = new_theme.id if edge.source_theme_id == old_theme_id else edge.source_theme_id
            new_target = new_theme.id if edge.target_theme_id == old_theme_id else edge.target_theme_id
            if new_source == new_target:
                continue
            if edge.relationship_type == ThemeRelationshipType.CHILD_OF and new_source == new_theme.id:
                # Parent override is handled below.
                continue
            key = (new_source, new_target, edge.relationship_type)
            if key in emitted:
                continue
            emitted.add(key)
            await self._add_relationship_row(
                codebook_id=codebook_id,
                source_theme_id=new_source,
                target_theme_id=new_target,
                relationship_type=edge.relationship_type,
                created_by=created_by,
                provenance=provenance,
            )

        if new_parent_theme_id is not None:
            await self._ensure_theme_in_codebook(codebook_id, new_parent_theme_id)
            await self._add_relationship_row(
                codebook_id=codebook_id,
                source_theme_id=new_theme.id,
                target_theme_id=new_parent_theme_id,
                relationship_type=ThemeRelationshipType.CHILD_OF,
                created_by=created_by,
                provenance=provenance,
            )
        else:
            old_parent_edges = [
                edge
                for edge in touched_edges
                if edge.relationship_type == ThemeRelationshipType.CHILD_OF
                and edge.source_theme_id == old_theme_id
            ]
            parent_ids = {edge.target_theme_id for edge in old_parent_edges}
            if len(parent_ids) > 1:
                options = ", ".join(map(str, sorted(parent_ids, key=str)))
                raise ThemeValidationError(
                    "replace_theme detected multiple parent candidates on old theme. "
                    f"Provide new_parent_theme_id explicitly. Candidates: {options}"
                )
            if len(parent_ids) == 1:
                await self._add_relationship_row(
                    codebook_id=codebook_id,
                    source_theme_id=new_theme.id,
                    target_theme_id=next(iter(parent_ids)),
                    relationship_type=ThemeRelationshipType.CHILD_OF,
                    created_by=created_by,
                    provenance=provenance,
                )

        old_theme.status = NodeStatus.DEPRECATED
        await self._finalize()
        return new_theme

    async def validate_theme_dag(
        self,
        *,
        codebook_id: UUID,
        theme_statuses: set[NodeStatus] | None = None,
        enforce_single_parent: bool = True,
        enforce_levels: bool = True,
    ) -> ThemeDagValidation:
        """
        Validate hierarchy constraints on active `CHILD_OF` links.

        Checks:
        - No cycles in the hierarchy.
        - Optional single-parent policy.
        - Optional level constraints (top-level themes cannot be children).
        """
        graph = await self.build_theme_dag(
            codebook_id=codebook_id,
            theme_statuses=theme_statuses,
            include_non_hierarchical=False,
        )

        violations: list[str] = []
        child_edges: list[ThemeEdgeView] = []
        for edges in graph.adjacency.values():
            for edge in edges:
                if edge.relationship_type == ThemeRelationshipType.CHILD_OF:
                    child_edges.append(edge)

        parent_count: defaultdict[UUID, int] = defaultdict(int)
        parent_to_children: defaultdict[UUID, set[UUID]] = defaultdict(set)
        for edge in child_edges:
            parent_count[edge.source_theme_id] += 1
            parent_to_children[edge.target_theme_id].add(edge.source_theme_id)

        if enforce_single_parent:
            duplicates = sorted((theme_id for theme_id, count in parent_count.items() if count > 1), key=str)
            if duplicates:
                joined = ", ".join(map(str, duplicates))
                violations.append(f"Themes with multiple active parents: {joined}")

        if enforce_levels:
            for edge in child_edges:
                child = graph.nodes.get(edge.source_theme_id)
                if child is None:
                    continue
                if child.level == ThemeLevel.THEME:
                    violations.append(f"Top-level theme {child.id} cannot be child_of another theme.")

        if self._contains_cycle(nodes=set(graph.nodes), parent_to_children=parent_to_children):
            violations.append("Hierarchy contains a cycle in active CHILD_OF relationships.")

        return ThemeDagValidation(is_valid=not violations, violations=tuple(violations))

    async def build_theme_dag(
        self,
        *,
        codebook_id: UUID,
        theme_statuses: set[NodeStatus] | None = None,
        include_non_hierarchical: bool = True,
    ) -> ThemeDagView:
        """
        Build a codebook-scoped graph projection from active database rows.

        By default this includes both hierarchy and semantic links. Set
        `include_non_hierarchical=False` for a pure hierarchy projection.
        """
        statuses = theme_statuses if theme_statuses is not None else set(DEFAULT_WORKING_THEME_STATUSES)
        nodes = await self._load_theme_nodes(codebook_id=codebook_id, theme_statuses=statuses)
        node_ids = set(nodes)

        relationships = await self._load_active_relationships_for_nodes(
            codebook_id=codebook_id,
            theme_ids=node_ids,
            include_non_hierarchical=include_non_hierarchical,
        )

        adjacency: dict[UUID, list[ThemeEdgeView]] = {theme_id: [] for theme_id in node_ids}
        for relationship in relationships:
            if relationship.source_theme_id not in adjacency:
                continue
            adjacency[relationship.source_theme_id].append(
                ThemeEdgeView(
                    source_theme_id=relationship.source_theme_id,
                    target_theme_id=relationship.target_theme_id,
                    relationship_type=relationship.relationship_type,
                    status=relationship.status,
                )
            )

        for edge_list in adjacency.values():
            edge_list.sort(key=lambda item: (item.relationship_type.value, item.target_theme_id))

        child_nodes = {
            edge.source_theme_id
            for edge_list in adjacency.values()
            for edge in edge_list
            if edge.relationship_type == ThemeRelationshipType.CHILD_OF
        }
        # Any node without an incoming CHILD_OF edge is a root in the hierarchy view.
        root_theme_ids = sorted(node_ids - child_nodes, key=str)
        return ThemeDagView(
            codebook_id=codebook_id,
            nodes=nodes,
            adjacency=adjacency,
            root_theme_ids=root_theme_ids,
        )

    async def get_theme_tree(
        self,
        *,
        codebook_id: UUID,
        root_theme_id: UUID | None = None,
        theme_statuses: set[NodeStatus] | None = None,
    ) -> list[ThemeTreeNode]:
        """
        Return a nested tree view built from active `CHILD_OF` relationships.

        This method intentionally validates first so callers do not get a
        partial or ambiguous tree when the graph violates hierarchy rules.
        """
        validation = await self.validate_theme_dag(
            codebook_id=codebook_id,
            theme_statuses=theme_statuses,
        )
        if not validation.is_valid:
            details = "; ".join(validation.violations)
            raise ThemeValidationError(f"Theme hierarchy is invalid: {details}")

        graph = await self.build_theme_dag(
            codebook_id=codebook_id,
            theme_statuses=theme_statuses,
            include_non_hierarchical=False,
        )
        parent_to_children: defaultdict[UUID, list[UUID]] = defaultdict(list)
        for edge_list in graph.adjacency.values():
            for edge in edge_list:
                if edge.relationship_type == ThemeRelationshipType.CHILD_OF:
                    parent_to_children[edge.target_theme_id].append(edge.source_theme_id)

        for parent_id, children in parent_to_children.items():
            children.sort(key=lambda child_id: graph.nodes[child_id].label.lower())

        if root_theme_id is not None:
            if root_theme_id not in graph.nodes:
                raise ThemeNotFoundError(f"Root theme '{root_theme_id}' not found in working graph.")
            root_ids = [root_theme_id]
        else:
            root_ids = graph.root_theme_ids

        return [self._build_tree_node(root_id, graph.nodes, parent_to_children, set()) for root_id in root_ids]

    async def auto_generate_theme_tree_for_codebook(
        self,
        *,
        codebook_id: UUID,
        root_theme_id: UUID | None = None,
        include_candidate_nodes: bool = True,
    ) -> list[ThemeTreeNode]:
        """
        Build a theme tree for one selected codebook from database state in one call.

        This is a convenience wrapper over `get_theme_tree` for callers that only
        provide a codebook ID and want an immediate hierarchy projection.
        """
        await self._ensure_codebook_exists(codebook_id)
        statuses = (
            set(DEFAULT_WORKING_THEME_STATUSES)
            if include_candidate_nodes
            else {NodeStatus.ACTIVE}
        )
        return await self.get_theme_tree(
            codebook_id=codebook_id,
            root_theme_id=root_theme_id,
            theme_statuses=statuses,
        )

    async def _ensure_codebook_exists(self, codebook_id: UUID) -> None:
        """Fail fast when the caller references a missing codebook."""
        stmt = select(Codebook.id).where(Codebook.id == codebook_id)
        row = (await self._session.execute(stmt)).scalar_one_or_none()
        if row is None:
            raise ThemeNotFoundError(f"Codebook '{codebook_id}' not found.")

    async def _ensure_theme_in_codebook(self, codebook_id: UUID, theme_id: UUID) -> Theme:
        """Resolve a theme only if it is an active member of the given codebook."""
        stmt = (
            select(Theme)
            .join(
                CodebookThemeRelationship,
                and_(
                    CodebookThemeRelationship.theme_id == Theme.id,
                    CodebookThemeRelationship.codebook_id == codebook_id,
                    CodebookThemeRelationship.relationship_type
                    == CodebookThemeRelationshipType.CONTAINS,
                    CodebookThemeRelationship.status == RelationshipStatus.ACTIVE,
                ),
            )
            .where(Theme.id == theme_id)
        )
        theme = (await self._session.execute(stmt)).scalar_one_or_none()
        if theme is None:
            raise ThemeNotFoundError(f"Theme '{theme_id}' not found in codebook '{codebook_id}'.")
        return theme

    async def _get_themes_in_codebook(
        self, *, codebook_id: UUID, theme_ids: list[UUID]
    ) -> list[Theme]:
        """Load a list of themes constrained to one codebook membership scope."""
        if not theme_ids:
            return []
        stmt = (
            select(Theme)
            .join(
                CodebookThemeRelationship,
                and_(
                    CodebookThemeRelationship.theme_id == Theme.id,
                    CodebookThemeRelationship.codebook_id == codebook_id,
                    CodebookThemeRelationship.relationship_type
                    == CodebookThemeRelationshipType.CONTAINS,
                    CodebookThemeRelationship.status == RelationshipStatus.ACTIVE,
                ),
            )
            .where(Theme.id.in_(theme_ids))
        )
        return list((await self._session.scalars(stmt)).all())

    async def _get_active_relationships(
        self,
        *,
        codebook_id: UUID,
        relationship_type: ThemeRelationshipType,
        source_theme_id: UUID | None = None,
        target_theme_id: UUID | None = None,
    ) -> list[ThemeRelationship]:
        """Load active relationships by type, with optional source/target filters."""
        stmt = select(ThemeRelationship).where(
            ThemeRelationship.codebook_id == codebook_id,
            ThemeRelationship.relationship_type == relationship_type,
            ThemeRelationship.status == RelationshipStatus.ACTIVE,
        )
        if source_theme_id is not None:
            stmt = stmt.where(ThemeRelationship.source_theme_id == source_theme_id)
        if target_theme_id is not None:
            stmt = stmt.where(ThemeRelationship.target_theme_id == target_theme_id)
        return list((await self._session.scalars(stmt)).all())

    async def _get_relationships_touching_themes(
        self, *, codebook_id: UUID, theme_ids: Iterable[UUID]
    ) -> list[ThemeRelationship]:
        """Load all active edges where any source or target is in the provided set."""
        ids = self._unique_ids(theme_ids)
        if not ids:
            return []
        stmt = select(ThemeRelationship).where(
            ThemeRelationship.codebook_id == codebook_id,
            ThemeRelationship.status == RelationshipStatus.ACTIVE,
            or_(
                ThemeRelationship.source_theme_id.in_(ids),
                ThemeRelationship.target_theme_id.in_(ids),
            ),
        )
        return list((await self._session.scalars(stmt)).all())

    async def _add_relationship_row(
        self,
        *,
        codebook_id: UUID,
        source_theme_id: UUID,
        target_theme_id: UUID,
        relationship_type: ThemeRelationshipType,
        created_by: ActorType,
        provenance: str | None = None,
    ) -> ThemeRelationship:
        """
        Insert an active relationship unless an equivalent active edge exists.

        For `CHILD_OF`, this method enforces:
        - no self edge,
        - single parent per child,
        - no cycle introduction.
        """
        if source_theme_id == target_theme_id:
            raise ThemeValidationError("Cannot create a self-referential theme relationship.")

        if relationship_type == ThemeRelationshipType.CHILD_OF:
            existing_parents = await self._get_active_relationships(
                codebook_id=codebook_id,
                source_theme_id=source_theme_id,
                relationship_type=ThemeRelationshipType.CHILD_OF,
            )
            conflicting_parents = {
                rel.target_theme_id
                for rel in existing_parents
                if rel.target_theme_id != target_theme_id
            }
            if conflicting_parents:
                parents = ", ".join(map(str, sorted(conflicting_parents, key=str)))
                raise ThemeValidationError(
                    f"Theme '{source_theme_id}' already has an active parent: {parents}"
                )
            await self._assert_no_cycle_on_child_of(
                codebook_id=codebook_id,
                child_id=source_theme_id,
                parent_id=target_theme_id,
            )

        existing_stmt = select(ThemeRelationship).where(
            ThemeRelationship.codebook_id == codebook_id,
            ThemeRelationship.source_theme_id == source_theme_id,
            ThemeRelationship.target_theme_id == target_theme_id,
            ThemeRelationship.relationship_type == relationship_type,
            ThemeRelationship.status == RelationshipStatus.ACTIVE,
        )
        existing = (await self._session.execute(existing_stmt)).scalar_one_or_none()
        if existing is not None:
            return existing

        relationship = ThemeRelationship(
            id=self._new_id(),
            codebook_id=codebook_id,
            source_theme_id=source_theme_id,
            target_theme_id=target_theme_id,
            relationship_type=relationship_type,
            status=RelationshipStatus.ACTIVE,
            created_by=created_by,
            provenance=provenance,
        )
        self._session.add(relationship)
        return relationship

    async def _assert_no_cycle_on_child_of(
        self, *, codebook_id: UUID, child_id: UUID, parent_id: UUID
    ) -> None:
        """
        Reject a new `CHILD_OF` edge if it creates a cycle.

        We compute reachability in the child-direction and ensure the proposed
        parent is not reachable from the proposed child.
        """
        if child_id == parent_id:
            raise ThemeValidationError("A theme cannot be parent/child of itself.")

        children_by_parent: defaultdict[UUID, set[UUID]] = defaultdict(set)
        stmt = select(ThemeRelationship.source_theme_id, ThemeRelationship.target_theme_id).where(
            ThemeRelationship.codebook_id == codebook_id,
            ThemeRelationship.relationship_type == ThemeRelationshipType.CHILD_OF,
            ThemeRelationship.status == RelationshipStatus.ACTIVE,
        )
        rows = (await self._session.execute(stmt)).all()
        for source_theme_id, target_theme_id in rows:
            children_by_parent[target_theme_id].add(source_theme_id)

        children_by_parent[parent_id].add(child_id)
        queue: deque[UUID] = deque([child_id])
        visited: set[UUID] = set()
        while queue:
            current = queue.popleft()
            if current == parent_id:
                raise ThemeValidationError(
                    f"Adding CHILD_OF({child_id} -> {parent_id}) would create a cycle."
                )
            if current in visited:
                continue
            visited.add(current)
            for descendant in children_by_parent.get(current, set()):
                if descendant not in visited:
                    queue.append(descendant)

    async def _remove_codebook_theme_membership(self, *, codebook_id: UUID, theme_id: UUID) -> None:
        """Mark active codebook-membership edges as removed for one theme."""
        stmt = select(CodebookThemeRelationship).where(
            CodebookThemeRelationship.codebook_id == codebook_id,
            CodebookThemeRelationship.theme_id == theme_id,
            CodebookThemeRelationship.relationship_type == CodebookThemeRelationshipType.CONTAINS,
            CodebookThemeRelationship.status == RelationshipStatus.ACTIVE,
        )
        relationships = list((await self._session.scalars(stmt)).all())
        for relationship in relationships:
            relationship.status = RelationshipStatus.REMOVED

    async def _deactivate_theme_relationships(self, *, codebook_id: UUID, theme_id: UUID) -> None:
        """Mark all active inbound/outbound theme edges as removed."""
        stmt = select(ThemeRelationship).where(
            ThemeRelationship.codebook_id == codebook_id,
            ThemeRelationship.status == RelationshipStatus.ACTIVE,
            or_(
                ThemeRelationship.source_theme_id == theme_id,
                ThemeRelationship.target_theme_id == theme_id,
            ),
        )
        relationships = list((await self._session.scalars(stmt)).all())
        for relationship in relationships:
            relationship.status = RelationshipStatus.REMOVED

    async def _hard_delete_theme(self, *, codebook_id: UUID, theme: Theme) -> None:
        """Physically remove a theme row only when it is safe across codebooks."""
        stmt = select(CodebookThemeRelationship).where(
            CodebookThemeRelationship.theme_id == theme.id,
            CodebookThemeRelationship.status == RelationshipStatus.ACTIVE,
        )
        active_memberships = list((await self._session.scalars(stmt)).all())
        active_codebook_ids = {row.codebook_id for row in active_memberships}
        if len(active_codebook_ids) > 1 or (
            len(active_codebook_ids) == 1 and codebook_id not in active_codebook_ids
        ):
            joined = ", ".join(map(str, sorted(active_codebook_ids, key=str)))
            raise ThemeConflictError(
                f"Cannot hard-delete theme '{theme.id}', still active in multiple codebooks: {joined}"
            )
        for membership in active_memberships:
            await self._session.delete(membership)

        await self._session.delete(theme)

    async def _load_theme_nodes(
        self, *, codebook_id: UUID, theme_statuses: set[NodeStatus]
    ) -> dict[UUID, ThemeNodeView]:
        """Load active node view models for graph projection."""
        if not theme_statuses:
            return {}
        stmt = (
            select(Theme)
            .join(
                CodebookThemeRelationship,
                and_(
                    CodebookThemeRelationship.theme_id == Theme.id,
                    CodebookThemeRelationship.codebook_id == codebook_id,
                    CodebookThemeRelationship.relationship_type
                    == CodebookThemeRelationshipType.CONTAINS,
                    CodebookThemeRelationship.status == RelationshipStatus.ACTIVE,
                ),
            )
            .where(Theme.status.in_(theme_statuses))
        )
        themes = list((await self._session.scalars(stmt)).all())
        return {
            theme.id: ThemeNodeView(
                id=theme.id,
                label=theme.label,
                description=theme.description,
                level=theme.level,
                status=theme.status,
                created_by=theme.created_by,
            )
            for theme in themes
        }

    async def _load_active_relationships_for_nodes(
        self,
        *,
        codebook_id: UUID,
        theme_ids: set[UUID],
        include_non_hierarchical: bool,
    ) -> list[ThemeRelationship]:
        """Load active edges where both endpoints are part of the selected node set."""
        if not theme_ids:
            return []
        stmt = select(ThemeRelationship).where(
            ThemeRelationship.codebook_id == codebook_id,
            ThemeRelationship.status == RelationshipStatus.ACTIVE,
            ThemeRelationship.source_theme_id.in_(theme_ids),
            ThemeRelationship.target_theme_id.in_(theme_ids),
        )
        if not include_non_hierarchical:
            stmt = stmt.where(ThemeRelationship.relationship_type == ThemeRelationshipType.CHILD_OF)
        return list((await self._session.scalars(stmt)).all())

    def _contains_cycle(
        self, *, nodes: set[UUID], parent_to_children: dict[UUID, set[UUID] | list[UUID]]
    ) -> bool:
        """
        Detect cycles using Kahn's topological-sort algorithm.

        If not all nodes can be visited with indegree-zero elimination, the
        hierarchy contains at least one cycle.
        """
        indegree: dict[UUID, int] = {node_id: 0 for node_id in nodes}
        for parent_id, children in parent_to_children.items():
            if parent_id not in indegree:
                continue
            for child_id in children:
                if child_id in indegree:
                    indegree[child_id] += 1

        queue: deque[UUID] = deque(node for node, degree in indegree.items() if degree == 0)
        visited = 0
        while queue:
            parent = queue.popleft()
            visited += 1
            for child in parent_to_children.get(parent, []):
                if child not in indegree:
                    continue
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        return visited != len(indegree)

    def _build_tree_node(
        self,
        theme_id: UUID,
        nodes: dict[UUID, ThemeNodeView],
        parent_to_children: dict[UUID, list[UUID]],
        path: set[UUID],
    ) -> ThemeTreeNode:
        """Recursively materialize one tree node and its descendants."""
        if theme_id in path:
            cycle_path = " -> ".join((*path, theme_id))
            raise ThemeValidationError(f"Cycle detected while building theme tree: {cycle_path}")
        if theme_id not in nodes:
            raise ThemeNotFoundError(f"Theme '{theme_id}' is missing from tree node set.")

        next_path = set(path)
        next_path.add(theme_id)
        children = [
            self._build_tree_node(child_id, nodes, parent_to_children, next_path)
            for child_id in parent_to_children.get(theme_id, [])
        ]
        return ThemeTreeNode(theme=nodes[theme_id], children=children)

    async def _finalize(self) -> None:
        """Commit immediately by default, or only flush in caller-managed transactions."""
        if self._auto_commit:
            await self._session.commit()
        else:
            await self._session.flush()

    def _create_theme_row(
        self,
        *,
        label: str,
        description: str,
        level: ThemeLevel,
        status: NodeStatus,
        created_by: ActorType,
    ) -> Theme:
        """Create a new in-memory `Theme` ORM row with a generated ID."""
        return Theme(
            id=self._new_id(),
            label=label,
            description=description,
            level=level,
            status=status,
            created_by=created_by,
        )

    @staticmethod
    def _new_id() -> UUID:
        return uuid4()

    @staticmethod
    def _unique_ids(values: Iterable[UUID]) -> list[UUID]:
        """Deduplicate while preserving caller-provided order."""
        return list(dict.fromkeys(values))
