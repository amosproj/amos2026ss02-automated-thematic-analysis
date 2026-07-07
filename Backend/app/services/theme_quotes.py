from __future__ import annotations

import math
from collections import defaultdict
from typing import Any
from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    CodebookApplicationRun,
    CorpusDocument,
    DocumentCoding,
    ThemeAssignment,
    ThemeHierarchyRelationship,
)
from app.models.demographic import DemographicRow
from app.schemas.common import Page, PageMeta
from app.schemas.theme_views import ThemeQuoteItem


class ThemeQuotesService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_theme_quotes(
        self,
        *,
        codebook_id: UUID,
        theme_id: UUID,
        page: int = 1,
        page_size: int = 20,
        application_run_id: UUID | None = None,
        include_descendants: bool = True,
    ) -> Page[ThemeQuoteItem]:
        run_id = await self._resolve_run_id(codebook_id, application_run_id)
        if run_id is None:
            return Page(items=[], meta=PageMeta(total=0, page=page, page_size=page_size, pages=0))

        theme_ids = await self._resolve_theme_ids(
            codebook_id=codebook_id,
            theme_id=theme_id,
            include_descendants=include_descendants,
        )

        base_filter = (
            DocumentCoding.application_run_id == run_id,
            ThemeAssignment.theme_id.in_(theme_ids),
            ThemeAssignment.is_present.is_(True),
            ThemeAssignment.quote.is_not(None),
        )

        total: int = (
            await self._session.execute(
                select(func.count()).select_from(
                    select(DocumentCoding.document_id, ThemeAssignment.quote)
                    .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
                    .where(*base_filter)
                    .group_by(DocumentCoding.document_id, ThemeAssignment.quote)
                    .subquery()
                )
            )
        ).scalar_one()

        pages = math.ceil(total / page_size) if total > 0 else 0
        offset = (page - 1) * page_size

        confidence = func.max(ThemeAssignment.confidence).label("confidence")
        page_rows = (
            await self._session.execute(
                select(
                    DocumentCoding.document_id,
                    ThemeAssignment.quote,
                    confidence,
                    CorpusDocument.title,
                    DemographicRow.interviewee_id,
                )
                .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
                .join(CorpusDocument, DocumentCoding.document_id == CorpusDocument.id)
                .outerjoin(DemographicRow, CorpusDocument.demographic_row_id == DemographicRow.id)
                .where(*base_filter)
                .group_by(
                    DocumentCoding.document_id,
                    ThemeAssignment.quote,
                    CorpusDocument.title,
                    DemographicRow.interviewee_id,
                )
                .order_by(desc(confidence), DocumentCoding.document_id, ThemeAssignment.quote)
                .offset(offset)
                .limit(page_size)
            )
        ).all()

        theme_ids_by_key = await self._load_tags_for_quotes(
            base_filter=base_filter,
            document_ids={row.document_id for row in page_rows},
        )

        items = [
            ThemeQuoteItem(
                quote=row.quote,
                confidence=row.confidence,
                document_id=row.document_id,
                document_title=row.title,
                interviewee_id=row.interviewee_id,
                theme_ids=theme_ids_by_key.get((row.document_id, row.quote), []),
            )
            for row in page_rows
        ]

        return Page(items=items, meta=PageMeta(total=total, page=page, page_size=page_size, pages=pages))

    async def _load_tags_for_quotes(
        self,
        *,
        base_filter: tuple[Any, ...],
        document_ids: set[UUID],
    ) -> dict[tuple[UUID, str], list[UUID]]:
        if not document_ids:
            return {}
        rows = (
            await self._session.execute(
                select(
                    DocumentCoding.document_id,
                    ThemeAssignment.quote,
                    ThemeAssignment.theme_id,
                )
                .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
                .where(*base_filter, DocumentCoding.document_id.in_(document_ids))
                .order_by(desc(ThemeAssignment.confidence), ThemeAssignment.id)
            )
        ).all()
        theme_ids_by_key: dict[tuple[UUID, str], list[UUID]] = defaultdict(list)
        for row in rows:
            ids = theme_ids_by_key[(row.document_id, row.quote)]
            if row.theme_id not in ids:
                ids.append(row.theme_id)
        return theme_ids_by_key

    async def _resolve_theme_ids(
        self,
        *,
        codebook_id: UUID,
        theme_id: UUID,
        include_descendants: bool,
    ) -> set[UUID]:
        if not include_descendants:
            return {theme_id}
        rows = (
            await self._session.execute(
                select(
                    ThemeHierarchyRelationship.parent_theme_id,
                    ThemeHierarchyRelationship.child_theme_id,
                ).where(
                    ThemeHierarchyRelationship.codebook_id == codebook_id,
                    ThemeHierarchyRelationship.is_active.is_(True),
                )
            )
        ).all()
        children_by_parent: dict[UUID, set[UUID]] = defaultdict(set)
        for parent_id, child_id in rows:
            children_by_parent[parent_id].add(child_id)

        resolved: set[UUID] = set()
        stack = [theme_id]
        while stack:
            current = stack.pop()
            if current in resolved:
                continue
            resolved.add(current)
            stack.extend(children_by_parent.get(current, ()))
        return resolved

    async def _resolve_run_id(self, codebook_id: UUID, application_run_id: UUID | None) -> UUID | None:
        if application_run_id is not None:
            return application_run_id
        return (
            await self._session.execute(
                select(CodebookApplicationRun.id)
                .where(
                    CodebookApplicationRun.codebook_id == codebook_id,
                    CodebookApplicationRun.status == "succeeded",
                )
                .order_by(desc(CodebookApplicationRun.finished_at), desc(CodebookApplicationRun.created_at))
                .limit(1)
            )
        ).scalar_one_or_none()
