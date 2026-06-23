from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from langchain_core.runnables import Runnable
from loguru import logger
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError, UnprocessableError
from app.llm.pipelines import (
    apply_codebook_with_codes_to_transcripts,
    build_codebook_application_with_codes_chain,
)
from app.models import (
    Code,
    CodeAssignment,
    Codebook,
    CodebookApplicationRun,
    CodebookThemeRelationship,
    Corpus,
    CorpusDocument,
    DocumentCoding,
    Theme,
    ThemeAssignment,
    ThemeCodeRelationship,
    ThemeHierarchyRelationship,
)
from app.schemas.llm import CodebookApplicationResult
from app.services.quote_matching import locate_quote_span

_APPLICATION_MAX_ATTEMPTS = 3
_APPLICATION_MAX_CONCURRENCY = 8
_APPLICATION_BATCH_SIZE = 16
_APPLICATION_RETRY_BASE_DELAY_S = 0.5
_APPLICATION_RETRY_MAX_DELAY_S = 5.0


class CodebookApplicationCancelledError(Exception):
    pass


@dataclass(frozen=True)
class _DocumentText:
    id: UUID
    title: str
    content: str


@dataclass(frozen=True)
class _ThemeRef:
    id: UUID
    label: str
    description: str | None
    path: tuple[str, ...]


@dataclass(frozen=True)
class _CodeRef:
    id: UUID
    label: str
    description: str | None
    theme_id: UUID | None


@dataclass(frozen=True)
class CodebookApplicationSummary:
    application_run: CodebookApplicationRun
    documents_total: int
    documents_coded: int
    documents_failed: int
    failed_documents: list[dict[str, str]]


class CodebookApplicationService:
    """Apply an existing codebook to selected transcripts and persist coded spans."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def apply_codebook(
        self,
        *,
        name: str | None = None,
        custom_id: str | None = None,
        corpus_id: UUID,
        codebook_id: UUID,
        transcript_document_ids: list[UUID] | None,
        provider: str | None = None,
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
        on_phase: Callable[[str], Awaitable[None]] | None = None,
        on_run_created: Callable[[UUID], Awaitable[None]] | None = None,
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
    ) -> CodebookApplicationSummary:
        normalized_document_ids = self._deduplicate_document_ids(transcript_document_ids)
        await self._load_corpus(corpus_id)
        codebook = await self._load_codebook(codebook_id=codebook_id, corpus_id=corpus_id)
        documents = await self._load_documents(
            corpus_id=corpus_id,
            transcript_document_ids=normalized_document_ids,
        )
        if not documents:
            raise UnprocessableError(
                "No transcripts found in the selected corpus. Please upload transcripts before applying a codebook."
            )

        if on_phase is not None:
            await on_phase("loading_codebook")
        themes, codes = await self._load_codebook_nodes(codebook_id=codebook.id)
        if not themes:
            raise UnprocessableError("The selected codebook has no active themes to apply.")
        codebook_context = self._build_codebook_context(themes=themes, codes=codes)
        themes_by_label = {self._label_key(theme.label): theme for theme in themes}
        codes_by_label = {self._label_key(code.label): code for code in codes}

        application_run = CodebookApplicationRun(
            id=uuid.uuid4(),
            name=name,
            custom_id=custom_id,
            corpus_id=corpus_id,
            codebook_id=codebook_id,
            status="running",
            documents_total=len(documents),
            documents_coded=0,
            documents_failed=0,
            started_at=_utc_now_naive(),
        )
        self._session.add(application_run)
        await self._session.commit()
        await self._session.refresh(application_run)
        application_run_id = application_run.id
        if on_run_created is not None:
            await on_run_created(application_run_id)

        # End the setup transaction before long LLM calls.
        await self._session.rollback()

        if on_progress is not None:
            await on_progress(0, len(documents))
        if on_phase is not None:
            await on_phase("coding_documents")

        chain = build_codebook_application_with_codes_chain(provider=provider)
        documents_done = 0
        documents_coded = 0
        failed_documents: list[dict[str, str]] = []

        # Process fixed-size index batches so result order can be mapped back to
        # the original document list after LangChain returns the batched output.
        for document_indexes in self._chunked(list(range(len(documents))), _APPLICATION_BATCH_SIZE):
            await self._raise_if_cancelled(should_cancel)
            batch_results = await self._apply_document_batch_with_retries(
                documents=[documents[index] for index in document_indexes],
                codebook_context=codebook_context,
                chain=chain,
                should_cancel=should_cancel,
            )
            for local_index, result in enumerate(batch_results):
                document = documents[document_indexes[local_index]]
                await self._raise_if_cancelled(should_cancel)
                try:
                    # Batch calls return exceptions as values so one failed
                    # document does not prevent successful documents persisting.
                    if isinstance(result, Exception):
                        raise result
                    await self._persist_successful_document_coding(
                        application_run_id=application_run_id,
                        codebook_id=codebook_id,
                        document=document,
                        result=result,
                        themes_by_label=themes_by_label,
                        codes_by_label=codes_by_label,
                    )
                    documents_coded += 1
                except CodebookApplicationCancelledError:
                    raise
                except Exception as exc:
                    await self._session.rollback()
                    logger.warning(
                        "Codebook application failed for document {} after retries: {}",
                        document.id,
                        exc,
                    )
                    await self._persist_failed_document_coding(
                        application_run_id=application_run_id,
                        codebook_id=codebook_id,
                        document=document,
                        error_message=str(exc),
                    )
                    failed_documents.append(
                        {
                            "document_id": str(document.id),
                            "title": document.title,
                            "error": str(exc),
                        }
                    )

                # Persist progress after each document, not after each batch, so
                # the job endpoint remains accurate during long application runs.
                documents_done += 1
                await self._update_run_counts(
                    application_run_id=application_run_id,
                    documents_coded=documents_coded,
                    documents_failed=len(failed_documents),
                    status="running",
                )
                if on_progress is not None:
                    await on_progress(documents_done, len(documents))

        await self._raise_if_cancelled(should_cancel)
        if on_phase is not None:
            await on_phase("persisting")
        application_run = await self._finish_run(
            application_run_id=application_run_id,
            documents_coded=documents_coded,
            documents_failed=len(failed_documents),
            status="succeeded",
        )
        return CodebookApplicationSummary(
            application_run=application_run,
            documents_total=len(documents),
            documents_coded=documents_coded,
            documents_failed=len(failed_documents),
            failed_documents=failed_documents,
        )

    @staticmethod
    def _deduplicate_document_ids(document_ids: list[UUID] | None) -> list[UUID]:
        if not document_ids:
            return []
        ordered_unique: list[UUID] = []
        seen: set[UUID] = set()
        for document_id in document_ids:
            if document_id in seen:
                continue
            seen.add(document_id)
            ordered_unique.append(document_id)
        return ordered_unique

    async def _load_corpus(self, corpus_id: UUID) -> Corpus:
        corpus = (
            await self._session.execute(
                select(Corpus).where(Corpus.id == corpus_id)
            )
        ).scalar_one_or_none()
        if corpus is None:
            raise NotFoundError(f"Corpus '{corpus_id}' not found")
        return corpus

    async def _load_codebook(self, *, codebook_id: UUID, corpus_id: UUID) -> Codebook:
        codebook = (
            await self._session.execute(
                select(Codebook).where(Codebook.id == codebook_id)
            )
        ).scalar_one_or_none()
        if codebook is None:
            raise NotFoundError(f"Codebook '{codebook_id}' not found")
        if codebook.corpus_id != corpus_id:
            raise UnprocessableError(
                f"Codebook '{codebook_id}' does not belong to corpus '{corpus_id}'"
            )
        return codebook

    async def _load_documents(
        self,
        *,
        corpus_id: UUID,
        transcript_document_ids: list[UUID],
    ) -> list[_DocumentText]:
        if not transcript_document_ids:
            documents = list(
                (
                    await self._session.scalars(
                        select(CorpusDocument)
                        .where(CorpusDocument.corpus_id == corpus_id)
                        .order_by(CorpusDocument.id)
                    )
                ).all()
            )
            return [
                _DocumentText(id=document.id, title=document.title, content=document.content or "")
                for document in documents
            ]

        documents = list(
            (
                await self._session.scalars(
                    select(CorpusDocument).where(
                        CorpusDocument.corpus_id == corpus_id,
                        CorpusDocument.id.in_(transcript_document_ids),
                    )
                )
            ).all()
        )
        documents_by_id = {document.id: document for document in documents}
        missing = [document_id for document_id in transcript_document_ids if document_id not in documents_by_id]
        if missing:
            missing_str = ", ".join(str(document_id) for document_id in missing)
            raise UnprocessableError(
                "Some transcript_document_ids were not found in the selected corpus: "
                f"{missing_str}"
            )
        return [
            _DocumentText(
                id=documents_by_id[document_id].id,
                title=documents_by_id[document_id].title,
                content=documents_by_id[document_id].content or "",
            )
            for document_id in transcript_document_ids
        ]

    async def _load_codebook_nodes(self, *, codebook_id: UUID) -> tuple[list[_ThemeRef], list[_CodeRef]]:
        themes = list(
            (
                await self._session.scalars(
                    select(Theme)
                    .join(
                        CodebookThemeRelationship,
                        and_(
                            CodebookThemeRelationship.theme_id == Theme.id,
                            CodebookThemeRelationship.codebook_id == codebook_id,
                            CodebookThemeRelationship.is_active.is_(True),
                        ),
                    )
                    .where(Theme.codebook_id == codebook_id, Theme.is_active.is_(True))
                    .distinct()
                    .order_by(Theme.label)
                )
            ).all()
        )
        hierarchy_edges = list(
            (
                await self._session.scalars(
                    select(ThemeHierarchyRelationship).where(
                        ThemeHierarchyRelationship.codebook_id == codebook_id,
                        ThemeHierarchyRelationship.is_active.is_(True),
                    )
                )
            ).all()
        )
        theme_path_by_id = self._theme_paths(themes=themes, hierarchy_edges=hierarchy_edges)
        theme_refs = [
            _ThemeRef(
                id=theme.id,
                label=theme.label,
                description=theme.description,
                path=theme_path_by_id.get(theme.id, (theme.label,)),
            )
            for theme in themes
        ]

        codes = list(
            (
                await self._session.scalars(
                    select(Code)
                    .where(Code.codebook_id == codebook_id, Code.is_active.is_(True))
                    .order_by(Code.label)
                )
            ).all()
        )
        code_theme_rows = list(
            (
                await self._session.scalars(
                    select(ThemeCodeRelationship).where(
                        ThemeCodeRelationship.codebook_id == codebook_id,
                        ThemeCodeRelationship.is_active.is_(True),
                    )
                )
            ).all()
        )
        theme_id_by_code_id = {row.code_id: row.theme_id for row in code_theme_rows}
        code_refs = [
            _CodeRef(
                id=code.id,
                label=code.label,
                description=code.description,
                theme_id=theme_id_by_code_id.get(code.id),
            )
            for code in codes
        ]
        return theme_refs, code_refs

    @staticmethod
    def _theme_paths(
        *,
        themes: list[Theme],
        hierarchy_edges: list[ThemeHierarchyRelationship],
    ) -> dict[UUID, tuple[str, ...]]:
        theme_by_id = {theme.id: theme for theme in themes}
        parent_by_child = {
            edge.child_theme_id: edge.parent_theme_id
            for edge in hierarchy_edges
            if edge.child_theme_id in theme_by_id and edge.parent_theme_id in theme_by_id
        }

        paths: dict[UUID, tuple[str, ...]] = {}
        for theme in themes:
            labels = [theme.label]
            seen = {theme.id}
            parent_id = parent_by_child.get(theme.id)
            while parent_id is not None and parent_id in theme_by_id and parent_id not in seen:
                seen.add(parent_id)
                labels.append(theme_by_id[parent_id].label)
                parent_id = parent_by_child.get(parent_id)
            paths[theme.id] = tuple(reversed(labels))
        return paths

    @staticmethod
    def _build_codebook_context(*, themes: list[_ThemeRef], codes: list[_CodeRef]) -> str:
        theme_by_id = {theme.id: theme for theme in themes}
        codes_by_theme_id: dict[UUID, list[_CodeRef]] = {}
        orphan_codes: list[_CodeRef] = []
        for code in codes:
            if code.theme_id is not None and code.theme_id in theme_by_id:
                codes_by_theme_id.setdefault(code.theme_id, []).append(code)
            else:
                orphan_codes.append(code)

        lines = [
            "Use only the exact theme and code labels listed below.",
            "",
            "THEMES AND CODES:",
        ]
        for theme in sorted(themes, key=lambda item: item.path):
            path = " > ".join(theme.path)
            lines.append(f"- Theme path: {path}")
            lines.append(f"  Theme label: {theme.label}")
            if theme.description:
                lines.append(f"  Theme definition: {theme.description}")
            theme_codes = sorted(codes_by_theme_id.get(theme.id, []), key=lambda item: item.label.lower())
            if theme_codes:
                lines.append("  Codes:")
                for code in theme_codes:
                    lines.append(f"    - Code label: {code.label}")
                    if code.description:
                        lines.append(f"      Code definition: {code.description}")
            lines.append("")

        if orphan_codes:
            lines.append("UNSCOPED CODES:")
            for code in sorted(orphan_codes, key=lambda item: item.label.lower()):
                lines.append(f"- Code label: {code.label}")
                if code.description:
                    lines.append(f"  Code definition: {code.description}")
        return "\n".join(lines).strip()

    async def _apply_document_batch_with_retries(
        self,
        *,
        documents: list[_DocumentText],
        codebook_context: str,
        chain: Runnable[dict[str, str], dict[str, Any]],
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
    ) -> list[CodebookApplicationResult | Exception]:
        started_at = time.monotonic()

        total = len(documents)
        results_by_index: dict[int, CodebookApplicationResult] = {}
        failures_by_index: dict[int, Exception] = {}
        attempts_by_index: dict[int, int] = {index: 0 for index in range(total)}
        pending_indexes: list[int] = []

        # Empty transcripts are deterministic input errors, so they are recorded
        # immediately and skipped instead of being sent to the LLM.
        for index, document in enumerate(documents):
            if not document.content.strip():
                failures_by_index[index] = ValueError("Transcript is empty.")
            else:
                pending_indexes.append(index)

        while pending_indexes:
            await self._raise_if_cancelled(should_cancel)
            retry_indexes: list[int] = []

            # Only unresolved documents are sent on each attempt. Successful
            # documents stay in results_by_index and are never billed again.
            batch_results = await apply_codebook_with_codes_to_transcripts(
                [documents[index].content for index in pending_indexes],
                codebook_context,
                chain=chain,
                max_concurrency=_APPLICATION_MAX_CONCURRENCY,
            )
            for local_index, result in enumerate(batch_results):
                document_index = pending_indexes[local_index]
                attempts_by_index[document_index] += 1
                attempt_count = attempts_by_index[document_index]

                if isinstance(result, CodebookApplicationResult):
                    results_by_index[document_index] = result
                    continue

                if attempt_count < _APPLICATION_MAX_ATTEMPTS:
                    retry_indexes.append(document_index)
                    logger.warning(
                        "Codebook application LLM call failed on attempt {}/{} for document {}: {}",
                        attempt_count,
                        _APPLICATION_MAX_ATTEMPTS,
                        documents[document_index].id,
                        result,
                    )
                    continue

                failures_by_index[document_index] = UnprocessableError(
                    f"LLM codebook application failed after {_APPLICATION_MAX_ATTEMPTS} attempts: {result}"
                )

            if not retry_indexes:
                break
            pending_indexes = retry_indexes

            # The retry set may contain documents with different attempt counts;
            # use the highest count so the shared sleep never under-backs off.
            retry_attempt = max(attempts_by_index[index] for index in retry_indexes)
            retry_delay = self._compute_retry_delay(attempt=retry_attempt)
            if retry_delay > 0:
                await asyncio.sleep(retry_delay)

        ordered_results: list[CodebookApplicationResult | Exception] = []
        for index in range(total):
            successful_result = results_by_index.get(index)
            if successful_result is not None:
                ordered_results.append(successful_result)
            else:
                # This fallback protects the caller from impossible states such
                # as a shortened LangChain result list.
                ordered_results.append(
                    failures_by_index.get(index) or UnprocessableError("Codebook application produced no result.")
                )

        logger.info(
            "Codebook application batch complete: documents={}, succeeded={}, failed={}, total_attempts={}, "
            "max_concurrency={}, batch_size={}, elapsed_s={:.2f}",
            total,
            len(results_by_index),
            total - len(results_by_index),
            sum(attempts_by_index.values()),
            _APPLICATION_MAX_CONCURRENCY,
            _APPLICATION_BATCH_SIZE,
            time.monotonic() - started_at,
        )
        return ordered_results

    @staticmethod
    def _chunked(items: list[int], chunk_size: int) -> list[list[int]]:
        return [items[index:index + chunk_size] for index in range(0, len(items), chunk_size)]

    async def _persist_successful_document_coding(
        self,
        *,
        application_run_id: UUID,
        codebook_id: UUID,
        document: _DocumentText,
        result: CodebookApplicationResult,
        themes_by_label: dict[str, _ThemeRef],
        codes_by_label: dict[str, _CodeRef],
    ) -> None:
        document_coding = DocumentCoding(
            id=uuid.uuid4(),
            application_run_id=application_run_id,
            document_id=document.id,
            codebook_id=codebook_id,
            status="coded",
            summary=result.summary,
            researcher_notes=result.researcher_notes,
        )
        self._session.add(document_coding)
        await self._session.flush()

        seen_theme_ids: set[UUID] = set()
        for theme_result in result.themes:
            theme = themes_by_label.get(self._label_key(theme_result.theme_label))
            if theme is None or theme.id in seen_theme_ids:
                continue
            seen_theme_ids.add(theme.id)
            quote_match = locate_quote_span(document.content, theme_result.quote) if theme_result.quote else None
            self._session.add(
                ThemeAssignment(
                    id=uuid.uuid4(),
                    document_coding_id=document_coding.id,
                    theme_id=theme.id,
                    is_present=theme_result.present,
                    confidence=self._clamp_confidence(theme_result.confidence),
                    quote=quote_match.quote if quote_match else None,
                    start_char=quote_match.start_char if quote_match else None,
                    end_char=quote_match.end_char if quote_match else None,
                    quote_match_status=quote_match.quote_match_status if quote_match else None,
                )
            )

        for code_result in result.codes:
            code = codes_by_label.get(self._label_key(code_result.code_label))
            if code is None or not code_result.quote.strip():
                continue
            theme_id = code.theme_id
            if code_result.theme_label:
                theme = themes_by_label.get(self._label_key(code_result.theme_label))
                if theme is not None:
                    theme_id = theme.id
            quote_match = locate_quote_span(document.content, code_result.quote)
            self._session.add(
                CodeAssignment(
                    id=uuid.uuid4(),
                    document_coding_id=document_coding.id,
                    code_id=code.id,
                    theme_id=theme_id,
                    quote=quote_match.quote,
                    start_char=quote_match.start_char,
                    end_char=quote_match.end_char,
                    quote_match_status=quote_match.quote_match_status,
                    confidence=self._clamp_confidence(code_result.confidence),
                    rationale=code_result.rationale,
                )
            )
            if theme_id is not None and theme_id not in seen_theme_ids:
                seen_theme_ids.add(theme_id)
                self._session.add(
                    ThemeAssignment(
                        id=uuid.uuid4(),
                        document_coding_id=document_coding.id,
                        theme_id=theme_id,
                        is_present=True,
                        confidence=self._clamp_confidence(code_result.confidence),
                        quote=quote_match.quote,
                        start_char=quote_match.start_char,
                        end_char=quote_match.end_char,
                        quote_match_status=quote_match.quote_match_status,
                    )
                )

        await self._session.commit()

    async def _persist_failed_document_coding(
        self,
        *,
        application_run_id: UUID,
        codebook_id: UUID,
        document: _DocumentText,
        error_message: str,
    ) -> None:
        self._session.add(
            DocumentCoding(
                id=uuid.uuid4(),
                application_run_id=application_run_id,
                document_id=document.id,
                codebook_id=codebook_id,
                status="failed",
                error_message=error_message,
            )
        )
        await self._session.commit()

    async def _update_run_counts(
        self,
        *,
        application_run_id: UUID,
        documents_coded: int,
        documents_failed: int,
        status: str,
    ) -> None:
        run = await self._session.get(CodebookApplicationRun, application_run_id)
        if run is None:
            return
        run.documents_coded = documents_coded
        run.documents_failed = documents_failed
        run.status = status
        await self._session.commit()

    async def _finish_run(
        self,
        *,
        application_run_id: UUID,
        documents_coded: int,
        documents_failed: int,
        status: str,
    ) -> CodebookApplicationRun:
        run = await self._session.get(CodebookApplicationRun, application_run_id)
        if run is None:
            raise NotFoundError(f"Codebook application run '{application_run_id}' not found")
        run.documents_coded = documents_coded
        run.documents_failed = documents_failed
        run.status = status
        run.finished_at = _utc_now_naive()
        await self._session.commit()
        return run

    @staticmethod
    async def _raise_if_cancelled(
        should_cancel: Callable[[], Awaitable[bool]] | None,
    ) -> None:
        if should_cancel is not None and await should_cancel():
            raise CodebookApplicationCancelledError("Codebook application was cancelled")

    @staticmethod
    def _label_key(value: str) -> str:
        return " ".join(value.casefold().split())

    @staticmethod
    def _clamp_confidence(value: float) -> float:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(1.0, numeric))

    @staticmethod
    def _compute_retry_delay(*, attempt: int) -> float:
        backoff = _APPLICATION_RETRY_BASE_DELAY_S * (2 ** max(0, attempt - 1))
        jitter = random.uniform(0.8, 1.2)
        return float(min(_APPLICATION_RETRY_MAX_DELAY_S, backoff * jitter))


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
