from __future__ import annotations

import math
from uuid import UUID

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import CodebookApplicationRun, CorpusDocument, DocumentCoding, ThemeAssignment
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
    ) -> Page[ThemeQuoteItem]:
        run_id = await self._resolve_run_id(codebook_id, application_run_id)
        if run_id is None:
            return Page(items=[], meta=PageMeta(total=0, page=page, page_size=page_size, pages=0))

        base_filter = (
            DocumentCoding.application_run_id == run_id,
            ThemeAssignment.theme_id == theme_id,
            ThemeAssignment.is_present.is_(True),
            ThemeAssignment.quote.is_not(None),
        )

        total: int = (
            await self._session.execute(
                select(func.count())
                .select_from(ThemeAssignment)
                .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
                .where(*base_filter)
            )
        ).scalar_one()

        pages = math.ceil(total / page_size) if total > 0 else 0
        offset = (page - 1) * page_size

        rows = (
            await self._session.execute(
                select(
                    ThemeAssignment.quote,
                    ThemeAssignment.confidence,
                    DocumentCoding.document_id,
                    CorpusDocument.title,
                    DemographicRow.interviewee_id,
                )
                .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
                .join(CorpusDocument, DocumentCoding.document_id == CorpusDocument.id)
                .outerjoin(DemographicRow, CorpusDocument.demographic_row_id == DemographicRow.id)
                .where(*base_filter)
                .order_by(desc(ThemeAssignment.confidence))
                .offset(offset)
                .limit(page_size)
            )
        ).all()

        items = [
            ThemeQuoteItem(
                quote=row.quote,
                confidence=row.confidence,
                document_id=row.document_id,
                document_title=row.title,
                interviewee_id=row.interviewee_id,
            )
            for row in rows
        ]

        return Page(items=items, meta=PageMeta(total=total, page=page, page_size=page_size, pages=pages))

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
