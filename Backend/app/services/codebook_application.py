from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError, UnprocessableError
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
from app.schemas.traceable_llm import (
    CodebookSynthesisResult,
    SynthesizedCode,
    SynthesizedThemeNode,
    SynthesizedThemePath,
)
from app.services.quote_matching import (
    QuoteSpanCandidate,
    select_deduplicated_quote_spans,
)
from app.services.traceable_analysis import (
    TraceableAnalysisCancelledError,
    TraceableAnalysisService,
    _AppliedEvidence,
)
from app.services.traceable_analysis import (
    _DocumentText as _TraceableDocumentText,
)


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
        self.traceable_service = TraceableAnalysisService(self._session)

    async def apply_codebook(
        self,
        *,
        name: str | None = None,
        custom_id: str | None = None,
        corpus_id: UUID,
        codebook_id: UUID,
        transcript_document_ids: list[UUID] | None,
        provider: str | None = None,
        on_progress: Callable[[int, int, int, int], Awaitable[None]] | None = None,
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
        themes_by_label = {self._label_key(theme.label): theme for theme in themes}
        codes_by_label = {self._label_key(code.label): code for code in codes}
        # The traceable application method expects an in-memory synthesis, not
        # ORM rows. Build that adapter once so standalone application and
        # generate+apply use the same deductive coding implementation.
        synthesis = self._build_traceable_synthesis(themes=themes, codes=codes)

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

        # Keep the run row visible to polling clients, then leave the session
        # outside a long transaction while the LLM work runs.
        await self._session.rollback()

        if on_phase is not None:
            await on_phase("coding_documents")

        try:
            # Reuse the paper-style application pass without invoking the
            # generation/refinement loop.
            application_result = await self.traceable_service._apply_codebook_to_documents(
                documents=[
                    _TraceableDocumentText(
                        id=document.id,
                        title=document.title,
                        content=document.content,
                    )
                    for document in documents
                ],
                synthesis=synthesis,
                should_cancel=should_cancel,
                provider=provider,
                on_progress=on_progress,
            )
        except TraceableAnalysisCancelledError as exc:
            raise CodebookApplicationCancelledError("Codebook application was cancelled") from exc

        await self._raise_if_cancelled(should_cancel)
        if on_phase is not None:
            await on_phase("persisting")
        documents_coded, failed_documents = await self._persist_traceable_application_result(
            application_run_id=application_run_id,
            codebook_id=codebook_id,
            documents=documents,
            applied_evidence=application_result.evidence,
            failed_document_ids=application_result.failed_document_ids,
            themes_by_label=themes_by_label,
            codes_by_label=codes_by_label,
        )
        application_run = await self._finish_run(
            application_run_id=application_run_id,
            documents_coded=documents_coded,
            documents_failed=len(failed_documents),
            status="succeeded",
            # Standalone application reports the tokens spent by this one
            # application job. Generate+apply handles full-job totals in
            # TraceableAnalysisService.
            llm_tokens_input=self.traceable_service.llm_tokens_input,
            llm_tokens_output=self.traceable_service.llm_tokens_output,
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
    def _build_traceable_synthesis(
        *,
        themes: list[_ThemeRef],
        codes: list[_CodeRef],
    ) -> CodebookSynthesisResult:
        """Adapt persisted codebook rows into the traceable application shape."""

        theme_by_id = {theme.id: theme for theme in themes}
        description_by_label = {
            CodebookApplicationService._label_key(theme.label): theme.description
            for theme in themes
            if theme.description
        }

        synthesis_themes: list[SynthesizedThemePath] = []
        seen_paths: set[tuple[str, ...]] = set()
        for theme in sorted(themes, key=lambda item: item.path):
            if theme.path in seen_paths:
                continue
            seen_paths.add(theme.path)
            synthesis_themes.append(
                SynthesizedThemePath(
                    path=[
                        SynthesizedThemeNode(
                            label=label,
                            description=description_by_label.get(
                                CodebookApplicationService._label_key(label)
                            ),
                        )
                        for label in theme.path
                    ]
                )
            )

        synthesis_codes: list[SynthesizedCode] = []
        needs_fallback_theme = False
        for code in sorted(codes, key=lambda item: item.label.lower()):
            code_theme = theme_by_id.get(code.theme_id) if code.theme_id is not None else None
            if code_theme is None:
                # The old schema permits orphan codes. The traceable prompt
                # needs a theme path, so keep these codes available under a
                # neutral fallback instead of silently dropping them.
                theme_path = ["Grounded Findings"]
                needs_fallback_theme = True
            else:
                theme_path = list(code_theme.path)
            synthesis_codes.append(
                SynthesizedCode(
                    code_label=code.label,
                    code_description=code.description,
                    theme_path=theme_path,
                )
            )

        if needs_fallback_theme and ("Grounded Findings",) not in seen_paths:
            synthesis_themes.append(
                SynthesizedThemePath(
                    path=[
                        SynthesizedThemeNode(
                            label="Grounded Findings",
                            description="Codes without an active theme relationship.",
                        )
                    ]
                )
            )

        return CodebookSynthesisResult(themes=synthesis_themes, codes=synthesis_codes)

    async def _persist_traceable_application_result(
        self,
        *,
        application_run_id: UUID,
        codebook_id: UUID,
        documents: list[_DocumentText],
        applied_evidence: list[_AppliedEvidence],
        failed_document_ids: list[UUID],
        themes_by_label: dict[str, _ThemeRef],
        codes_by_label: dict[str, _CodeRef],
    ) -> tuple[int, list[dict[str, str]]]:
        """Persist traceable evidence into the legacy application tables."""

        evidence_by_document: dict[UUID, list[_AppliedEvidence]] = defaultdict(list)
        for evidence in applied_evidence:
            evidence_by_document[evidence.document_id].append(evidence)

        failed_ids = set(failed_document_ids)
        documents_coded = 0
        failed_documents: list[dict[str, str]] = []
        for document in documents:
            document_evidence = evidence_by_document.get(document.id, [])
            document_failed = document.id in failed_ids
            document_coding = DocumentCoding(
                id=uuid.uuid4(),
                application_run_id=application_run_id,
                document_id=document.id,
                codebook_id=codebook_id,
                status="failed" if document_failed else "coded",
                summary=next(
                    (evidence.summary for evidence in document_evidence if evidence.summary),
                    f"Traceable application assigned {len(document_evidence)} grounded quote-code pairs.",
                ),
                researcher_notes=next(
                    (evidence.researcher_notes for evidence in document_evidence if evidence.researcher_notes),
                    None,
                ),
                error_message=(
                    "Traceable application response could not be parsed after retries."
                    if document_failed
                    else None
                ),
            )
            self._session.add(document_coding)
            await self._session.flush()

            if document_failed:
                failed_documents.append(
                    {
                        "document_id": str(document.id),
                        "title": document.title,
                        "error": document_coding.error_message or "Application failed.",
                    }
                )
                await self._update_run_counts(
                    application_run_id=application_run_id,
                    documents_coded=documents_coded,
                    documents_failed=len(failed_documents),
                    status="running",
                )
                continue

            resolved_assignments: list[tuple[_AppliedEvidence, _CodeRef, UUID | None]] = []
            for evidence in document_evidence:
                code = codes_by_label.get(self._label_key(evidence.code_label))
                if code is None:
                    # The LLM is instructed to use exact labels, but this guard
                    # keeps persistence robust if a provider returns a renamed
                    # or malformed code after retries.
                    continue
                theme_id = code.theme_id
                if evidence.theme_label:
                    theme = themes_by_label.get(self._label_key(evidence.theme_label))
                    if theme is not None:
                        theme_id = theme.id
                resolved_assignments.append((evidence, code, theme_id))

            # The application pass can return the same passage several times
            # for one theme — verbatim under two codes or as overlapping
            # spans. Persist each passage once per theme so read views (e.g.
            # transcript highlights) do not stack duplicate quotes.
            kept_indices = select_deduplicated_quote_spans(
                [
                    QuoteSpanCandidate(
                        group_key=theme_id if theme_id is not None else ("code", code.id),
                        quote=evidence.quote,
                        start_char=evidence.start_char,
                        end_char=evidence.end_char,
                        confidence=self._clamp_confidence(evidence.confidence),
                    )
                    for evidence, code, theme_id in resolved_assignments
                ]
            )

            seen_theme_ids: set[UUID] = set()
            for index in kept_indices:
                evidence, code, theme_id = resolved_assignments[index]
                self._session.add(
                    CodeAssignment(
                        id=uuid.uuid4(),
                        document_coding_id=document_coding.id,
                        code_id=code.id,
                        theme_id=theme_id,
                        quote=evidence.quote,
                        start_char=evidence.start_char,
                        end_char=evidence.end_char,
                        quote_match_status=evidence.quote_match_status,
                        confidence=self._clamp_confidence(evidence.confidence),
                        rationale=evidence.rationale,
                    )
                )
                if theme_id is not None and theme_id not in seen_theme_ids:
                    # Store one positive theme assignment per document/theme
                    # even if several code assignments under that theme match.
                    seen_theme_ids.add(theme_id)
                    self._session.add(
                        ThemeAssignment(
                            id=uuid.uuid4(),
                            document_coding_id=document_coding.id,
                            theme_id=theme_id,
                            is_present=True,
                            confidence=self._clamp_confidence(evidence.confidence),
                            quote=evidence.quote,
                            start_char=evidence.start_char,
                            end_char=evidence.end_char,
                            quote_match_status=evidence.quote_match_status,
                        )
                    )

            documents_coded += 1
            await self._update_run_counts(
                application_run_id=application_run_id,
                documents_coded=documents_coded,
                documents_failed=len(failed_documents),
                status="running",
            )

        await self._session.commit()
        return documents_coded, failed_documents

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
        llm_tokens_input: int | None = None,
        llm_tokens_output: int | None = None,
    ) -> CodebookApplicationRun:
        run = await self._session.get(CodebookApplicationRun, application_run_id)
        if run is None:
            raise NotFoundError(f"Codebook application run '{application_run_id}' not found")
        run.documents_coded = documents_coded
        run.documents_failed = documents_failed
        run.status = status
        run.finished_at = _utc_now_naive()
        run.llm_tokens_input = llm_tokens_input
        run.llm_tokens_output = llm_tokens_output
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


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
