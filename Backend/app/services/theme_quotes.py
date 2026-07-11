from __future__ import annotations

import math
from collections import defaultdict
from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    CodebookApplicationRun,
    CorpusDocument,
    DocumentCoding,
    ThemeAssignment,
)
from app.models.demographic import DemographicRow
from app.schemas.common import Page, PageMeta
from app.schemas.theme_views import ThemeQuoteItem
from app.services.theme_hierarchy import load_descendants_and_self


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

        theme_ids = (
            await load_descendants_and_self(self._session, codebook_id=codebook_id, theme_id=theme_id)
            if include_descendants
            else {theme_id}
        )

        from sqlalchemy import union_all

        from app.models import CodeAssignment

        theme_stmt = select(
            DocumentCoding.document_id,
            ThemeAssignment.quote,
            ThemeAssignment.confidence,
            ThemeAssignment.theme_id.label("node_id"),
        ).join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id).where(
            DocumentCoding.application_run_id == run_id,
            ThemeAssignment.theme_id.in_(theme_ids),
            ThemeAssignment.is_present.is_(True),
            ThemeAssignment.quote.is_not(None),
        )

        code_stmt = select(
            DocumentCoding.document_id,
            CodeAssignment.quote,
            CodeAssignment.confidence,
            CodeAssignment.code_id.label("node_id"),
        ).join(DocumentCoding, CodeAssignment.document_coding_id == DocumentCoding.id).where(
            DocumentCoding.application_run_id == run_id,
            CodeAssignment.code_id.in_(theme_ids),
            CodeAssignment.quote.is_not(None),
        )

        unified_assignment = union_all(theme_stmt, code_stmt).subquery("unified_assignment")

        total: int = (
            await self._session.execute(
                select(func.count()).select_from(
                    select(unified_assignment.c.document_id, unified_assignment.c.quote)
                    .group_by(unified_assignment.c.document_id, unified_assignment.c.quote)
                    .subquery()
                )
            )
        ).scalar_one()

        pages = math.ceil(total / page_size) if total > 0 else 0
        offset = (page - 1) * page_size

        confidence_col = func.max(unified_assignment.c.confidence).label("confidence")
        page_rows = (
            await self._session.execute(
                select(
                    unified_assignment.c.document_id,
                    unified_assignment.c.quote,
                    confidence_col,
                    CorpusDocument.title,
                    DemographicRow.interviewee_id,
                )
                .join(CorpusDocument, unified_assignment.c.document_id == CorpusDocument.id)
                .outerjoin(DemographicRow, CorpusDocument.demographic_row_id == DemographicRow.id)
                .group_by(
                    unified_assignment.c.document_id,
                    unified_assignment.c.quote,
                    CorpusDocument.title,
                    DemographicRow.interviewee_id,
                )
                .order_by(desc(confidence_col), unified_assignment.c.document_id, unified_assignment.c.quote)
                .offset(offset)
                .limit(page_size)
            )
        ).all()

        theme_ids_by_key = await self._load_tags_for_quotes(
            run_id=run_id,
            document_ids={row.document_id for row in page_rows},
            node_ids=theme_ids,
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
        run_id: UUID,
        document_ids: set[UUID],
        node_ids: set[UUID],
    ) -> dict[tuple[UUID, str], list[UUID]]:
        if not document_ids:
            return {}

        from sqlalchemy import union_all

        from app.models import CodeAssignment

        theme_stmt = select(
            DocumentCoding.document_id,
            ThemeAssignment.quote,
            ThemeAssignment.theme_id.label("node_id"),
            ThemeAssignment.confidence,
            ThemeAssignment.id,
        ).join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id).where(
            DocumentCoding.application_run_id == run_id,
            DocumentCoding.document_id.in_(document_ids),
            ThemeAssignment.theme_id.in_(node_ids),
            ThemeAssignment.is_present.is_(True),
            ThemeAssignment.quote.is_not(None),
        )

        code_stmt = select(
            DocumentCoding.document_id,
            CodeAssignment.quote,
            CodeAssignment.code_id.label("node_id"),
            CodeAssignment.confidence,
            CodeAssignment.id,
        ).join(DocumentCoding, CodeAssignment.document_coding_id == DocumentCoding.id).where(
            DocumentCoding.application_run_id == run_id,
            DocumentCoding.document_id.in_(document_ids),
            CodeAssignment.code_id.in_(node_ids),
            CodeAssignment.quote.is_not(None),
        )

        unified = union_all(theme_stmt, code_stmt).subquery("unified")

        rows = (
            await self._session.execute(
                select(
                    unified.c.document_id,
                    unified.c.quote,
                    unified.c.node_id,
                )
                .order_by(desc(unified.c.confidence), unified.c.id)
            )
        ).all()
        theme_ids_by_key: dict[tuple[UUID, str], list[UUID]] = defaultdict(list)
        for row in rows:
            ids = theme_ids_by_key[(row.document_id, row.quote)]
            if row.node_id not in ids:
                ids.append(row.node_id)
        return theme_ids_by_key

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
