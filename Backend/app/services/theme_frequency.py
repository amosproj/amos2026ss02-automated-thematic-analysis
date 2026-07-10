from __future__ import annotations

from uuid import UUID

from sqlalchemy import and_, desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Code,
    CodeAssignment,
    Codebook,
    CodebookApplicationRun,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    DocumentCoding,
    Theme,
    ThemeAssignment,
)
from app.schemas.theme_views import ThemeFrequencyItem
from app.services.theme_graph import ThemeNotFoundError
from app.services.theme_hierarchy import descendants_and_self, load_children_map


class ThemeFrequencyService:
    """Build a flat, codebook-scoped theme frequency list."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_theme_frequencies(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None = None,
    ) -> list[ThemeFrequencyItem]:
        await self._ensure_codebook_exists(codebook_id=codebook_id)
        selected_run_id = await self._resolve_application_run_id(
            codebook_id=codebook_id,
            application_run_id=application_run_id,
        )
        nodes = await self._load_active_nodes(codebook_id=codebook_id)
        node_ids = {node.id for node in nodes}
        document_ids_by_node_id = await self._load_document_ids_by_node_id(
            codebook_id=codebook_id,
            application_run_id=selected_run_id,
            node_ids=node_ids,
        )

        children_map = await load_children_map(self._session, codebook_id=codebook_id)
        parent_occurrence_by_node_id = {
            node_id: len(
                set().union(
                    *(document_ids_by_node_id.get(d, set()) for d in descendants_and_self(node_id, children_map))
                )
            )
            for node_id in node_ids
        }
        total_interviews_in_corpus = await self._load_total_interviews_in_corpus(
            codebook_id=codebook_id,
            application_run_id=selected_run_id,
        )

        payload = []
        for node in nodes:
            own_occurrence = len(document_ids_by_node_id.get(node.id, set()))
            parent_occurrence = parent_occurrence_by_node_id.get(node.id, own_occurrence)
            payload.append(
                ThemeFrequencyItem(
                    theme_id=node.id,
                    theme_name=node.label,
                    occurrence_count=own_occurrence,
                    interview_coverage_percentage=self._to_coverage_percentage(
                        occurrence_count=own_occurrence,
                        total_interviews=total_interviews_in_corpus,
                    ),
                    parent_occurrence_count=parent_occurrence,
                    parent_interview_coverage_percentage=self._to_coverage_percentage(
                        occurrence_count=parent_occurrence,
                        total_interviews=total_interviews_in_corpus,
                    ),
                )
            )
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

    async def _resolve_application_run_id(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None,
    ) -> UUID | None:
        if application_run_id is not None:
            run = await self._session.get(CodebookApplicationRun, application_run_id)
            if run is None or run.codebook_id != codebook_id:
                raise ThemeNotFoundError(
                    f"Codebook application run '{application_run_id}' not found for codebook '{codebook_id}'."
                )
            return run.id

        stmt = (
            select(CodebookApplicationRun.id)
            .where(
                CodebookApplicationRun.codebook_id == codebook_id,
                CodebookApplicationRun.status == "succeeded",
            )
            .order_by(desc(CodebookApplicationRun.finished_at), desc(CodebookApplicationRun.created_at))
            .limit(1)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def _load_active_nodes(self, *, codebook_id: UUID) -> list[Theme | Code]:
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
            .distinct()
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
            .distinct()
        )
        codes = list((await self._session.scalars(stmt_codes)).all())
        
        return themes + codes

    async def _load_document_ids_by_node_id(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None,
        node_ids: set[UUID],
    ) -> dict[UUID, set[UUID]]:
        del codebook_id
        if application_run_id is None or not node_ids:
            return {node_id: set() for node_id in node_ids}
            
        stmt_themes = (
            select(ThemeAssignment.theme_id.label("node_id"), DocumentCoding.document_id)
            .join(DocumentCoding, ThemeAssignment.document_coding_id == DocumentCoding.id)
            .where(
                DocumentCoding.application_run_id == application_run_id,
                ThemeAssignment.theme_id.in_(node_ids),
                ThemeAssignment.is_present.is_(True),
            )
            .distinct()
        )
        
        stmt_codes = (
            select(CodeAssignment.code_id.label("node_id"), DocumentCoding.document_id)
            .join(DocumentCoding, CodeAssignment.document_coding_id == DocumentCoding.id)
            .where(
                DocumentCoding.application_run_id == application_run_id,
                CodeAssignment.code_id.in_(node_ids),
            )
            .distinct()
        )
        
        from sqlalchemy import union
        stmt = union(stmt_themes, stmt_codes)
        
        rows = (await self._session.execute(stmt)).all()
        document_ids: dict[UUID, set[UUID]] = {node_id: set() for node_id in node_ids}
        for node_id, document_id in rows:
            document_ids[node_id].add(document_id)
        return document_ids

    async def _load_total_interviews_in_corpus(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID | None,
    ) -> int:
        del codebook_id
        if application_run_id is None:
            return 0
        run = await self._session.get(CodebookApplicationRun, application_run_id)
        if run is None:
            return 0
        return run.documents_total

    @staticmethod
    def _to_coverage_percentage(*, occurrence_count: int, total_interviews: int) -> float:
        if total_interviews <= 0:
            return 0.0
        return (occurrence_count / total_interviews) * 100.0
