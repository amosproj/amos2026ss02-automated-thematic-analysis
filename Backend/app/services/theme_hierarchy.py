"""
Shared helpers for walking a codebook's theme hierarchy.
For coverage, quotes and demographic breakdowns.
"""

from __future__ import annotations

from collections import defaultdict
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ThemeHierarchyRelationship


async def load_children_map(
    session: AsyncSession, *, codebook_id: UUID
) -> dict[UUID, set[UUID]]:
    """Active parent-theme -> {child-theme} edges for one codebook."""
    rows = (
        await session.execute(
            select(
                ThemeHierarchyRelationship.parent_theme_id,
                ThemeHierarchyRelationship.child_theme_id,
            ).where(
                ThemeHierarchyRelationship.codebook_id == codebook_id,
                ThemeHierarchyRelationship.is_active.is_(True),
            )
        )
    ).all()

    # Also load code relationships so codes are descendants of themes
    from app.models.code import ThemeCodeRelationship
    code_rows = (
        await session.execute(
            select(
                ThemeCodeRelationship.theme_id,
                ThemeCodeRelationship.code_id,
            ).where(
                ThemeCodeRelationship.codebook_id == codebook_id,
                ThemeCodeRelationship.is_active.is_(True),
            )
        )
    ).all()

    children: dict[UUID, set[UUID]] = defaultdict(set)
    for parent_id, child_id in rows:
        children[parent_id].add(child_id)
        
    for parent_id, child_id in code_rows:
        children[parent_id].add(child_id)

    return dict(children)


def descendants_and_self(theme_id: UUID, children_map: dict[UUID, set[UUID]]) -> set[UUID]:
    """A theme plus every descendant; the visited set tolerates cyclic data."""
    resolved: set[UUID] = set()
    stack = [theme_id]
    while stack:
        current = stack.pop()
        if current in resolved:
            continue
        resolved.add(current)
        stack.extend(children_map.get(current, ()))
    return resolved


async def load_descendants_and_self(
    session: AsyncSession, *, codebook_id: UUID, theme_id: UUID
) -> set[UUID]:
    """A theme + all its descendants, resolved from the codebook's active hierarchy."""
    return descendants_and_self(theme_id, await load_children_map(session, codebook_id=codebook_id))
