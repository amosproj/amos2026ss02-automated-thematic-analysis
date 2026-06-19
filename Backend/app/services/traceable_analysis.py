from __future__ import annotations

import asyncio
import json
import math
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from langchain_core.output_parsers import JsonOutputParser
from loguru import logger
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.exceptions import NotFoundError, UnprocessableError
from app.llm.client import build_chat_model
from app.llm.traceable_prompts import (
    build_batch_code_relationship_prompt,
    build_code_relationship_prompt,
    build_codebook_review_prompt,
    build_missing_code_generation_prompt,
    build_quote_code_extraction_prompt,
    build_research_query_block,
    build_researcher_topics_block,
    build_subtheme_synthesis_prompt,
    build_theme_synthesis_prompt,
    build_traceable_application_prompt,
)
from app.models import (
    Code,
    CodeAssignment,
    Codebook,
    CodebookApplicationRun,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Corpus,
    CorpusDocument,
    DocumentCoding,
    Theme,
    ThemeAssignment,
    ThemeCodeRelationship,
    ThemeHierarchyRelationship,
)
from app.schemas.traceable_analysis import TraceableAnalysisResult
from app.schemas.traceable_llm import (
    BatchCodeRelationshipResults,
    CodebookReviewResult,
    CodebookSynthesisResult,
    CodeRelationshipResult,
    MissingCodeGenerationResult,
    QuoteCodeExtractionResult,
    SubthemeSynthesisResult,
    SynthesizedCode,
    SynthesizedThemeNode,
    SynthesizedThemePath,
    ThemeSynthesisResult,
    TraceableApplicationResult,
)
from app.services.quote_matching import locate_quote_span
from app.services.traceable_code_consolidation import (
    CodeCandidate,
    ConsolidatedCode,
    consolidate_code_candidates,
)


class TraceableAnalysisCancelledError(Exception):
    pass


_RELATIONSHIP_CLASSIFICATION_MAX_ATTEMPTS = 3
_APPLICATION_MAX_ATTEMPTS = 3


@dataclass(frozen=True)
class _DocumentText:
    id: UUID
    title: str
    content: str


@dataclass
class _QuoteEvidence:
    quote_id: str
    document_id: UUID
    quote: str
    start_char: int | None
    end_char: int | None
    quote_match_status: str
    candidate_id: str
    code_label: str
    code_description: str | None
    confidence: float
    rationale: str | None


@dataclass
class _AppliedEvidence:
    document_id: UUID
    code_label: str
    theme_label: str | None
    quote: str
    start_char: int | None
    end_char: int | None
    quote_match_status: str
    confidence: float
    rationale: str | None
    summary: str | None = None
    researcher_notes: str | None = None


@dataclass
class _ApplicationPassResult:
    evidence: list[_AppliedEvidence]
    failed_document_ids: list[UUID]


@dataclass(frozen=True)
class _PersistedCodebookRefs:
    codebook: Codebook
    theme_by_label: dict[str, Theme]
    code_by_label: dict[str, Code]
    theme_id_by_code_label: dict[str, UUID | None]


@dataclass
class _IterationArtifact:
    iteration: int
    synthesis: CodebookSynthesisResult
    consolidated_codes: list[ConsolidatedCode]
    evaluation_evidence: list[_AppliedEvidence]
    metrics: dict[str, float | int | bool]
    action_log: list[dict[str, object]]


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class TraceableAnalysisService:
    """Experimental quote-grounded codebook generation plus application.

    The pipeline follows the paper's overall shape while adapting persistence to
    the existing codebook/application tables: quote-code evidence first, code
    consolidation, code->subtheme->theme synthesis, reviewer refinement, then a
    final fixed-codebook application pass.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._code_relationship_chain = None
        self._batch_code_relationship_chain = None

    async def run_analysis(
        self,
        *,
        codebook_name: str,
        analysis_name: str | None,
        custom_id: str | None,
        corpus_id: UUID,
        transcript_document_ids: list[UUID] | None,
        research_query: str | None = None,
        researcher_topics: str | None = None,
        max_refinement_rounds: int = 1,
        on_unit_progress: Callable[[int, int], Awaitable[None]] | None = None,
        on_phase_progress: Callable[[str, int, int], Awaitable[None]] | None = None,
        on_phase: Callable[[str], Awaitable[None]] | None = None,
        on_codebook_created: Callable[[UUID], Awaitable[None]] | None = None,
        on_application_run_created: Callable[[UUID], Awaitable[None]] | None = None,
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
    ) -> TraceableAnalysisResult:
        normalized_document_ids = self._deduplicate_document_ids(transcript_document_ids)
        logger.info(
            "Traceable analysis started: corpus_id={}, selected_documents={}, codebook_name='{}', "
            "max_refinement_rounds={}, research_query_present={}, researcher_topics_present={}",
            corpus_id,
            len(normalized_document_ids) if normalized_document_ids else "all",
            codebook_name,
            max_refinement_rounds,
            bool(research_query),
            bool(researcher_topics),
        )
        await self._load_corpus(corpus_id)
        documents = await self._load_documents(
            corpus_id=corpus_id,
            transcript_document_ids=normalized_document_ids,
        )
        documents = [document for document in documents if document.content.strip()]
        if not documents:
            raise UnprocessableError("No non-empty transcripts found for traceable analysis.")
        logger.info(
            "Traceable analysis loaded documents: corpus_id={}, documents={}",
            corpus_id,
            len(documents),
        )

        await self._session.rollback()
        training_documents, heldout_documents = self._split_train_heldout(documents)
        evaluation_documents = heldout_documents or training_documents
        logger.info(
            "Traceable train/heldout split: training_documents={}, heldout_documents={}, evaluation_documents={}",
            len(training_documents),
            len(heldout_documents),
            len(evaluation_documents),
        )

        if on_phase is not None:
            await on_phase("extracting_quote_codes")
        # Paper stage: extract grounded evidence before any theme synthesis.
        # This is the main guard against zero-shot theme hallucination.
        quote_evidence = await self._extract_quote_codes(
            documents=training_documents,
            research_query=research_query,
            researcher_topics=researcher_topics,
            on_unit_progress=on_unit_progress,
            should_cancel=should_cancel,
        )
        if not quote_evidence:
            raise UnprocessableError("Traceable analysis extracted no grounded quote-code pairs.")
        logger.info(
            "Traceable extraction complete: training_documents={}, quote_code_pairs={}, unique_initial_codes={}",
            len(training_documents),
            len(quote_evidence),
            len({self._label_key(evidence.code_label) for evidence in quote_evidence}),
        )

        action_log: list[dict[str, object]] = [
            {
                "action": "extract_quote_code_pairs",
                "documents": len(training_documents),
                "heldout_documents": len(heldout_documents),
                "quotes": len(quote_evidence),
            }
        ]
        candidates = self._build_code_candidates(quote_evidence)
        if on_phase is not None:
            await on_phase("consolidating_codes")
        await self._raise_if_cancelled(should_cancel)
        # Paper stage: use embeddings to shortlist likely duplicate/related
        # codes, then let the LLM classify only those candidate relationships.
        consolidated_codes, consolidation_log = await consolidate_code_candidates(
            candidates,
            classifier=self._classify_code_pair,
            batch_classifier=self._classify_code_pairs,
            on_pair_progress=(
                (lambda done, total: on_phase_progress("consolidating_codes", done, total))
                if on_phase_progress is not None
                else None
            ),
        )
        action_log.extend(consolidation_log)
        if not consolidated_codes:
            raise UnprocessableError("Code consolidation produced no usable codes.")
        logger.info(
            "Traceable code consolidation complete: initial_candidates={}, consolidated_codes={}, actions={}",
            len(candidates),
            len(consolidated_codes),
            len(consolidation_log),
        )

        if on_phase is not None:
            await on_phase("synthesizing_themes")
        # Paper stage: synthesize upward from consolidated codes. The helper
        # does this in two prompts: codes->subthemes, then subthemes->themes.
        synthesis = await self._synthesize_codebook(
            consolidated_codes=consolidated_codes,
            quote_evidence=quote_evidence,
            research_query=research_query,
            researcher_topics=researcher_topics,
        )
        synthesis = self._ensure_synthesis_covers_codes(synthesis, consolidated_codes)
        logger.info(
            "Traceable synthesis complete: theme_paths={}, codes={}",
            len(synthesis.themes),
            len(synthesis.codes),
        )
        action_log.append(
            {
                "action": "synthesize_codebook",
                "themes": len(synthesis.themes),
                "codes": len(synthesis.codes),
            }
        )
        if on_phase is not None:
            await on_phase("evaluating_iterations")
        selected_iteration, iteration_artifacts, iteration_log = await self._select_best_iteration(
            synthesis=synthesis,
            consolidated_codes=consolidated_codes,
            quote_evidence=quote_evidence,
            training_documents=training_documents,
            evaluation_documents=evaluation_documents,
            used_heldout=bool(heldout_documents),
            research_query=research_query,
            researcher_topics=researcher_topics,
            max_refinement_rounds=max_refinement_rounds,
            should_cancel=should_cancel,
        )
        synthesis = selected_iteration.synthesis
        consolidated_codes = selected_iteration.consolidated_codes
        action_log.extend(iteration_log)
        logger.info(
            "Traceable iteration selection complete: selected_iteration={}, composite_score={:.3f}, "
            "theme_paths={}, codes={}",
            selected_iteration.iteration,
            float(selected_iteration.metrics.get("composite_score", 0.0)),
            len(synthesis.themes),
            len(synthesis.codes),
        )

        if on_phase is not None:
            await on_phase("persisting_codebook")
        # Adaptation: the paper's artifacts are persisted into the existing
        # Codebook/Theme/Code tables so the current UI can read the result.
        persisted = await self._persist_codebook(
            codebook_name=codebook_name,
            corpus_id=corpus_id,
            research_query=research_query,
            researcher_topics=researcher_topics,
            synthesis=synthesis,
        )
        if on_codebook_created is not None:
            await on_codebook_created(persisted.codebook.id)
        logger.info(
            "Traceable codebook persisted: codebook_id={}, themes={}, codes={}",
            persisted.codebook.id,
            len(persisted.theme_by_label),
            len(persisted.code_by_label),
        )

        if on_phase is not None:
            await on_phase("applying_codebook")
        # Final paper-style application: after refinement, apply only the fixed
        # generated codebook. Generation quotes are provenance, not assignments.
        application_result = await self._apply_codebook_to_documents(
            documents=documents,
            synthesis=synthesis,
            should_cancel=should_cancel,
        )
        applied_evidence = application_result.evidence
        action_log.append(
            {
                "action": "apply_final_codebook",
                "documents": len(documents),
                "assignments": len(applied_evidence),
                "documents_failed": len(application_result.failed_document_ids),
            }
        )
        logger.info(
            "Traceable final application complete: documents={}, assignments={}, failed_documents={}",
            len(documents),
            len(applied_evidence),
            len(application_result.failed_document_ids),
        )
        application_run = await self._persist_application(
            analysis_name=analysis_name,
            custom_id=custom_id,
            corpus_id=corpus_id,
            documents=documents,
            applied_evidence=applied_evidence,
            failed_document_ids=application_result.failed_document_ids,
            persisted=persisted,
        )
        if on_application_run_created is not None:
            await on_application_run_created(application_run.id)
        logger.info(
            "Traceable analysis finished: codebook_id={}, application_run_id={}, documents_coded={}, "
            "documents_failed={}, final_themes={}, final_codes={}",
            persisted.codebook.id,
            application_run.id,
            application_run.documents_coded,
            application_run.documents_failed,
            len(persisted.theme_by_label),
            len(persisted.code_by_label),
        )

        provenance = self._build_provenance_payload(
            quote_evidence=quote_evidence,
            consolidated_codes=consolidated_codes,
            synthesis=synthesis,
            applied_evidence=applied_evidence,
            iteration_artifacts=iteration_artifacts,
            selected_iteration=selected_iteration.iteration,
            used_heldout_evaluation=bool(heldout_documents),
            final_failed_document_ids=application_result.failed_document_ids,
        )
        action_log = self._with_action_ids(action_log)
        return TraceableAnalysisResult(
            codebook_id=persisted.codebook.id,
            application_run_id=application_run.id,
            documents_processed=len(documents),
            analysis_units_processed=len(documents),
            quotes_created=len(quote_evidence),
            codes_created=len(persisted.code_by_label),
            themes_created=len(persisted.theme_by_label),
            documents_coded=application_run.documents_coded,
            documents_failed=application_run.documents_failed,
            provenance=provenance,
            action_log=action_log,
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

    @staticmethod
    def _split_train_heldout(documents: list[_DocumentText]) -> tuple[list[_DocumentText], list[_DocumentText]]:
        cfg = get_settings()
        if len(documents) < 3 or cfg.TRACEABLE_HELDOUT_RATIO <= 0:
            return documents, []
        heldout_count = max(1, int(round(len(documents) * min(cfg.TRACEABLE_HELDOUT_RATIO, 0.5))))
        heldout_ids = {
            document.id
            for index, document in enumerate(documents)
            if index >= len(documents) - heldout_count
        }
        training = [document for document in documents if document.id not in heldout_ids]
        heldout = [document for document in documents if document.id in heldout_ids]
        if not training:
            return documents, []
        return training, heldout

    async def _load_corpus(self, corpus_id: UUID) -> Corpus:
        corpus = (
            await self._session.execute(select(Corpus).where(Corpus.id == corpus_id))
        ).scalar_one_or_none()
        if corpus is None:
            raise NotFoundError(f"Corpus '{corpus_id}' not found")
        return corpus

    async def _load_documents(
        self,
        *,
        corpus_id: UUID,
        transcript_document_ids: list[UUID],
    ) -> list[_DocumentText]:
        if not transcript_document_ids:
            rows = list(
                (
                    await self._session.scalars(
                        select(CorpusDocument)
                        .where(CorpusDocument.corpus_id == corpus_id)
                        .order_by(CorpusDocument.id)
                    )
                ).all()
            )
            return [_DocumentText(id=row.id, title=row.title, content=row.content or "") for row in rows]

        rows = list(
            (
                await self._session.scalars(
                    select(CorpusDocument).where(
                        CorpusDocument.corpus_id == corpus_id,
                        CorpusDocument.id.in_(transcript_document_ids),
                    )
                )
            ).all()
        )
        by_id = {row.id: row for row in rows}
        missing = [document_id for document_id in transcript_document_ids if document_id not in by_id]
        if missing:
            missing_str = ", ".join(str(document_id) for document_id in missing)
            raise UnprocessableError(
                "Some transcript_document_ids were not found in the selected corpus: "
                f"{missing_str}"
            )
        return [
            _DocumentText(
                id=by_id[document_id].id,
                title=by_id[document_id].title,
                content=by_id[document_id].content or "",
            )
            for document_id in transcript_document_ids
        ]

    async def _extract_quote_codes(
        self,
        *,
        documents: list[_DocumentText],
        research_query: str | None,
        researcher_topics: str | None,
        on_unit_progress: Callable[[int, int], Awaitable[None]] | None,
        should_cancel: Callable[[], Awaitable[bool]] | None,
    ) -> list[_QuoteEvidence]:
        parser = JsonOutputParser(pydantic_object=QuoteCodeExtractionResult)
        chain = build_quote_code_extraction_prompt() | build_chat_model() | parser
        evidence: list[_QuoteEvidence] = []
        if on_unit_progress is not None:
            await on_unit_progress(0, len(documents))

        for index, document in enumerate(documents, start=1):
            await self._raise_if_cancelled(should_cancel)
            raw_result = await chain.ainvoke(
                {
                    "transcript": document.content,
                    "research_query_block": build_research_query_block(research_query),
                    "researcher_topics_block": build_researcher_topics_block(researcher_topics),
                }
            )
            result = QuoteCodeExtractionResult(**raw_result)
            document_pairs_before = len(evidence)
            for pair_index, pair in enumerate(result.quote_code_pairs, start=1):
                label = self._normalize_label(pair.code_label)
                quote = pair.quote.strip()
                if not label or not quote:
                    continue
                match = locate_quote_span(document.content, quote)
                candidate_id = f"{document.id}:candidate:{pair_index}:{self._label_key(label)}"
                evidence.append(
                    _QuoteEvidence(
                        quote_id=f"{document.id}:quote:{pair_index}:{uuid.uuid4()}",
                        document_id=document.id,
                        quote=match.quote,
                        start_char=match.start_char,
                        end_char=match.end_char,
                        quote_match_status=match.quote_match_status,
                        candidate_id=candidate_id,
                        code_label=label,
                        code_description=self._clean_optional_text(pair.code_description),
                        confidence=self._clamp_confidence(pair.confidence),
                        rationale=self._clean_optional_text(pair.rationale),
                    )
                )
            if on_unit_progress is not None:
                await on_unit_progress(index, len(documents))
            logger.info(
                "Traceable extraction document complete: document_index={}, documents_total={}, "
                "document_id={}, quote_code_pairs={}",
                index,
                len(documents),
                document.id,
                len(evidence) - document_pairs_before,
            )
        return evidence

    def _build_code_candidates(self, quote_evidence: list[_QuoteEvidence]) -> list[CodeCandidate]:
        grouped: dict[str, list[_QuoteEvidence]] = defaultdict(list)
        for evidence in quote_evidence:
            grouped[self._label_key(evidence.code_label)].append(evidence)

        candidates: list[CodeCandidate] = []
        for label_key, group in grouped.items():
            preferred = max(group, key=lambda item: len(item.code_description or ""))
            candidates.append(
                CodeCandidate(
                    candidate_id=label_key,
                    label=preferred.code_label,
                    description=preferred.code_description,
                    quote_ids=[item.quote_id for item in group],
                )
            )
        return candidates

    async def _classify_code_pair(
        self,
        left: CodeCandidate,
        right: CodeCandidate,
    ) -> CodeRelationshipResult:
        if self._code_relationship_chain is None:
            parser = JsonOutputParser(pydantic_object=CodeRelationshipResult)
            self._code_relationship_chain = build_code_relationship_prompt() | build_chat_model(temperature=0.0) | parser
        payload = {
            "label_a": left.label,
            "description_a": left.description or "",
            "label_b": right.label,
            "description_b": right.description or "",
        }
        for attempt in range(1, _RELATIONSHIP_CLASSIFICATION_MAX_ATTEMPTS + 1):
            try:
                raw_result = await self._code_relationship_chain.ainvoke(payload)
                return CodeRelationshipResult(**raw_result)
            except Exception as exc:
                if attempt >= _RELATIONSHIP_CLASSIFICATION_MAX_ATTEMPTS:
                    logger.warning(
                        "Traceable pair classification failed after retries; using conservative fallback: "
                        "code_a='{}', code_b='{}', error={}",
                        left.label,
                        right.label,
                        exc,
                    )
                    return CodeRelationshipResult(
                        relationship="orthogonal",
                        confidence=0.0,
                        reason=f"Classification failed after retries: {type(exc).__name__}",
                    )
                logger.warning(
                    "Traceable pair classification retry: attempt={}, code_a='{}', code_b='{}', error={}",
                    attempt,
                    left.label,
                    right.label,
                    exc,
                )
                await asyncio.sleep(0.5 * attempt)

    async def _classify_code_pairs(
        self,
        pairs: list[tuple[int, CodeCandidate, CodeCandidate]],
    ) -> dict[int, CodeRelationshipResult]:
        if self._batch_code_relationship_chain is None:
            parser = JsonOutputParser(pydantic_object=BatchCodeRelationshipResults)
            self._batch_code_relationship_chain = (
                build_batch_code_relationship_prompt() | build_chat_model(temperature=0.0) | parser
            )
        pairs_payload = [
            {
                "pair_id": pair_id,
                "code_a": {
                    "label": left.label,
                    "description": left.description or "",
                },
                "code_b": {
                    "label": right.label,
                    "description": right.description or "",
                },
            }
            for pair_id, left, right in pairs
        ]
        payload = {"pairs_json": json.dumps(pairs_payload, ensure_ascii=False)}
        for attempt in range(1, _RELATIONSHIP_CLASSIFICATION_MAX_ATTEMPTS + 1):
            try:
                raw_result = await self._batch_code_relationship_chain.ainvoke(payload)
                parsed = BatchCodeRelationshipResults(**raw_result)
                return {
                    item.pair_id: CodeRelationshipResult(
                        relationship=item.relationship,
                        confidence=item.confidence,
                        reason=item.reason,
                    )
                    for item in parsed.pairs
                }
            except Exception as exc:
                if attempt >= _RELATIONSHIP_CLASSIFICATION_MAX_ATTEMPTS:
                    logger.warning(
                        "Traceable batch pair classification failed after retries: pairs={}, error={}",
                        len(pairs),
                        exc,
                    )
                    raise
                logger.warning(
                    "Traceable batch pair classification retry: attempt={}, pairs={}, error={}",
                    attempt,
                    len(pairs),
                    exc,
                )
                await asyncio.sleep(0.5 * attempt)
        return {}

    async def _synthesize_codebook(
        self,
        *,
        consolidated_codes: list[ConsolidatedCode],
        quote_evidence: list[_QuoteEvidence],
        research_query: str | None,
        researcher_topics: str | None,
    ) -> CodebookSynthesisResult:
        quote_by_id = {quote.quote_id: quote for quote in quote_evidence}
        payload = []
        for code in consolidated_codes:
            # Limit examples per code to keep the synthesis prompt bounded
            # while still preserving direct evidence for each concept.
            examples = [quote_by_id[quote_id].quote for quote_id in code.quote_ids[:5] if quote_id in quote_by_id]
            payload.append(
                {
                    "code_label": code.label,
                    "code_description": code.description,
                    "frequency": code.frequency,
                    "example_quotes": examples,
                }
            )

        subtheme_parser = JsonOutputParser(pydantic_object=SubthemeSynthesisResult)
        subtheme_chain = build_subtheme_synthesis_prompt() | build_chat_model(temperature=0.0) | subtheme_parser
        raw_subthemes = await subtheme_chain.ainvoke(
            {
                "codes": json.dumps(payload, ensure_ascii=True, indent=2),
                "research_query_block": build_research_query_block(research_query),
                "researcher_topics_block": build_researcher_topics_block(researcher_topics),
            }
        )
        subthemes = SubthemeSynthesisResult(**raw_subthemes)
        subthemes = self._ensure_subthemes_cover_codes(subthemes, consolidated_codes)
        logger.info(
            "Traceable subtheme synthesis complete: consolidated_codes={}, subthemes={}",
            len(consolidated_codes),
            len(subthemes.subthemes),
        )

        theme_parser = JsonOutputParser(pydantic_object=ThemeSynthesisResult)
        theme_chain = build_theme_synthesis_prompt() | build_chat_model(temperature=0.0) | theme_parser
        raw_themes = await theme_chain.ainvoke(
            {
                "subthemes": json.dumps(subthemes.model_dump(mode="json"), ensure_ascii=True, indent=2),
                "research_query_block": build_research_query_block(research_query),
                "researcher_topics_block": build_researcher_topics_block(researcher_topics),
            }
        )
        themes = ThemeSynthesisResult(**raw_themes)
        themes = self._ensure_themes_cover_subthemes(themes, subthemes)
        logger.info(
            "Traceable theme synthesis complete: subthemes={}, themes={}",
            len(subthemes.subthemes),
            len(themes.themes),
        )
        return self._compose_codebook_synthesis(
            consolidated_codes=consolidated_codes,
            subthemes=subthemes,
            themes=themes,
        )

    def _ensure_synthesis_covers_codes(
        self,
        synthesis: CodebookSynthesisResult,
        consolidated_codes: list[ConsolidatedCode],
    ) -> CodebookSynthesisResult:
        # LLM synthesis can omit or rename codes. The paper requires traceable
        # code artifacts, so canonical consolidated labels are enforced here.
        canonical_by_key = {self._label_key(code.label): code for code in consolidated_codes}
        returned: set[str] = set()
        themes = list(synthesis.themes)
        codes: list[SynthesizedCode] = []
        for synthesized_code in synthesis.codes:
            canonical = canonical_by_key.get(self._label_key(synthesized_code.code_label))
            if canonical is None:
                continue
            returned.add(self._label_key(canonical.label))
            codes.append(
                SynthesizedCode(
                    code_label=canonical.label,
                    code_description=canonical.description or synthesized_code.code_description,
                    theme_path=synthesized_code.theme_path,
                )
            )
        if not themes:
            themes.append(
                SynthesizedThemePath(
                    path=[SynthesizedThemeNode(label="Grounded Findings", description="Codes grounded in transcript evidence.")]
                )
            )
        fallback_path = [node.label for node in themes[0].path] or ["Grounded Findings"]
        for code in consolidated_codes:
            if self._label_key(code.label) in returned:
                continue
            codes.append(
                SynthesizedCode(
                    code_label=code.label,
                    code_description=code.description,
                    theme_path=fallback_path,
                )
            )
        return CodebookSynthesisResult(themes=themes, codes=codes)

    def _ensure_subthemes_cover_codes(
        self,
        subthemes: SubthemeSynthesisResult,
        consolidated_codes: list[ConsolidatedCode],
    ) -> SubthemeSynthesisResult:
        # Repair pass: every consolidated code must remain reachable from a
        # subtheme, even if the model drops it in the first synthesis response.
        canonical_by_key = {self._label_key(code.label): code for code in consolidated_codes}
        covered: set[str] = set()
        cleaned_subthemes = []
        for subtheme in subthemes.subthemes:
            code_labels = []
            for raw_label in subtheme.code_labels:
                canonical = canonical_by_key.get(self._label_key(raw_label))
                if canonical is None:
                    continue
                covered.add(self._label_key(canonical.label))
                if canonical.label not in code_labels:
                    code_labels.append(canonical.label)
            if code_labels and subtheme.subtheme_label.strip():
                cleaned_subthemes.append(
                    subtheme.model_copy(update={"code_labels": code_labels})
                )

        missing = [
            code for code in consolidated_codes
            if self._label_key(code.label) not in covered
        ]
        if missing:
            cleaned_subthemes.append(
                {
                    "subtheme_label": "Grounded Evidence Patterns",
                    "subtheme_description": "Consolidated codes grounded in transcript evidence.",
                    "code_labels": [code.label for code in missing],
                }
            )
        return SubthemeSynthesisResult(subthemes=cleaned_subthemes)

    def _ensure_themes_cover_subthemes(
        self,
        themes: ThemeSynthesisResult,
        subthemes: SubthemeSynthesisResult,
    ) -> ThemeSynthesisResult:
        # Repair pass: every subtheme must be attached to a root theme so the
        # persisted hierarchy stays navigable as a tree.
        subtheme_by_key = {
            self._label_key(subtheme.subtheme_label): subtheme
            for subtheme in subthemes.subthemes
        }
        covered: set[str] = set()
        cleaned_themes = []
        for theme in themes.themes:
            labels = []
            for raw_label in theme.subtheme_labels:
                subtheme = subtheme_by_key.get(self._label_key(raw_label))
                if subtheme is None:
                    continue
                covered.add(self._label_key(subtheme.subtheme_label))
                if subtheme.subtheme_label not in labels:
                    labels.append(subtheme.subtheme_label)
            if labels and theme.theme_label.strip():
                cleaned_themes.append(theme.model_copy(update={"subtheme_labels": labels}))

        missing = [
            subtheme.subtheme_label
            for subtheme in subthemes.subthemes
            if self._label_key(subtheme.subtheme_label) not in covered
        ]
        if missing:
            cleaned_themes.append(
                {
                    "theme_label": "Grounded Findings",
                    "theme_description": "Themes synthesized from grounded transcript codes.",
                    "subtheme_labels": missing,
                }
            )
        return ThemeSynthesisResult(themes=cleaned_themes)

    def _compose_codebook_synthesis(
        self,
        *,
        consolidated_codes: list[ConsolidatedCode],
        subthemes: SubthemeSynthesisResult,
        themes: ThemeSynthesisResult,
    ) -> CodebookSynthesisResult:
        # Collapse the paper's separate theme/subtheme/code artifacts into the
        # existing flat ThemePath + Code schema used by persistence and UI code.
        code_by_key = {self._label_key(code.label): code for code in consolidated_codes}
        subtheme_by_key = {
            self._label_key(subtheme.subtheme_label): subtheme
            for subtheme in subthemes.subthemes
        }
        theme_for_subtheme: dict[str, tuple[str, str | None]] = {}
        theme_paths: list[SynthesizedThemePath] = []
        seen_paths: set[tuple[str, ...]] = set()
        for theme in themes.themes:
            for subtheme_label in theme.subtheme_labels:
                subtheme = subtheme_by_key.get(self._label_key(subtheme_label))
                if subtheme is None:
                    continue
                theme_for_subtheme[self._label_key(subtheme.subtheme_label)] = (
                    theme.theme_label,
                    theme.theme_description,
                )
                path_key = (theme.theme_label, subtheme.subtheme_label)
                if path_key in seen_paths:
                    continue
                seen_paths.add(path_key)
                theme_paths.append(
                    SynthesizedThemePath(
                        path=[
                            SynthesizedThemeNode(
                                label=theme.theme_label,
                                description=theme.theme_description,
                            ),
                            SynthesizedThemeNode(
                                label=subtheme.subtheme_label,
                                description=subtheme.subtheme_description,
                            ),
                        ]
                    )
                )

        synthesized_codes: list[SynthesizedCode] = []
        for subtheme in subthemes.subthemes:
            theme_label, _theme_description = theme_for_subtheme.get(
                self._label_key(subtheme.subtheme_label),
                ("Grounded Findings", "Themes synthesized from grounded transcript codes."),
            )
            for raw_code_label in subtheme.code_labels:
                code = code_by_key.get(self._label_key(raw_code_label))
                if code is None:
                    continue
                synthesized_codes.append(
                    SynthesizedCode(
                        code_label=code.label,
                        code_description=code.description,
                        theme_path=[theme_label, subtheme.subtheme_label],
                    )
                )
        return CodebookSynthesisResult(themes=theme_paths, codes=synthesized_codes)

    async def _select_best_iteration(
        self,
        *,
        synthesis: CodebookSynthesisResult,
        consolidated_codes: list[ConsolidatedCode],
        quote_evidence: list[_QuoteEvidence],
        training_documents: list[_DocumentText],
        evaluation_documents: list[_DocumentText],
        used_heldout: bool,
        research_query: str | None,
        researcher_topics: str | None,
        max_refinement_rounds: int,
        should_cancel: Callable[[], Awaitable[bool]] | None,
    ) -> tuple[_IterationArtifact, list[_IterationArtifact], list[dict[str, object]]]:
        cfg = get_settings()
        max_iterations = max(1, min(cfg.TRACEABLE_MAX_ITERATIONS, max_refinement_rounds + 1))
        current = synthesis
        current_codes = list(consolidated_codes)
        best: _IterationArtifact | None = None
        artifacts: list[_IterationArtifact] = []
        action_log: list[dict[str, object]] = []

        for iteration in range(1, max_iterations + 1):
            await self._raise_if_cancelled(should_cancel)
            evaluation_result = await self._apply_codebook_to_documents(
                documents=evaluation_documents,
                synthesis=current,
                should_cancel=should_cancel,
            )
            evaluation_evidence = evaluation_result.evidence
            metrics = self._compute_iteration_metrics(
                synthesis=current,
                consolidated_codes=current_codes,
                quote_evidence=quote_evidence,
                evaluation_documents=evaluation_documents,
                evaluation_evidence=evaluation_evidence,
                used_heldout=used_heldout,
                failed_document_count=len(evaluation_result.failed_document_ids),
            )
            artifact = _IterationArtifact(
                iteration=iteration,
                synthesis=current,
                consolidated_codes=list(current_codes),
                evaluation_evidence=evaluation_evidence,
                metrics=metrics,
                action_log=[],
            )
            artifacts.append(artifact)
            action_log.append(
                {
                    "action": "evaluate_iteration",
                    "iteration": iteration,
                    "outputs": {"metrics": metrics},
                }
            )
            logger.info(
                "Traceable iteration evaluated: iteration={}, composite={:.3f}, reusability={:.3f}, "
                "coverage={:.3f}, parsimony={:.3f}, consistency={:.3f}, codes={}",
                iteration,
                float(metrics["composite_score"]),
                float(metrics["code_reusability"]),
                float(metrics["document_coverage"]),
                float(metrics["parsimony_score"]),
                float(metrics["train_eval_consistency"]),
                int(metrics["code_count"]),
            )
            if best is None or float(metrics["composite_score"]) > float(best.metrics["composite_score"]):
                best = artifact

            if iteration >= max_iterations:
                break

            before_labels = self._codebook_label_set(current)
            review = await self._review_codebook(
                synthesis=current,
                quote_evidence=quote_evidence,
                consolidated_codes=current_codes,
                round_index=iteration,
                metrics=metrics,
            )
            if not review.actions:
                action_log.append({"action": "review_complete", "round": iteration, "edits": 0})
                logger.info(
                    "Traceable iteration refinement complete: iteration={}, proposed_actions=0, status=no_edits",
                    iteration,
                )
                break
            current, applied_actions = self._apply_review_actions(current, review, round_index=iteration)
            artifact.action_log.extend(applied_actions)
            action_log.extend(applied_actions)
            deleted_code_keys = {
                self._label_key(str(action.get("target")))
                for action in applied_actions
                if action.get("applied")
                and action.get("action") == "delete"
                and action.get("artifact_type") == "code"
                and action.get("target")
            }
            if deleted_code_keys:
                current_codes = [
                    code for code in current_codes
                    if self._label_key(code.label) not in deleted_code_keys
                ]
            current_codes = self._apply_code_merge_actions_to_consolidated_codes(
                current_codes,
                applied_actions,
            )

            if any(action.action == "generate" for action in review.actions):
                missing_codes = await self._generate_missing_codes(
                    synthesis=current,
                    quote_evidence=quote_evidence,
                    existing_codes=current_codes,
                    round_index=iteration,
                )
                if missing_codes:
                    existing_keys = {self._label_key(code.label) for code in current_codes}
                    additions = [
                        code for code in missing_codes
                        if self._label_key(code.label) not in existing_keys
                    ]
                    if additions:
                        current_codes.extend(additions)
                        generated_actions = [
                            {
                                "action": "generate_grounded_code",
                                "round": iteration,
                                "target": code.label,
                                "outputs": {"quote_ids": code.quote_ids},
                            }
                            for code in additions
                        ]
                        artifact.action_log.extend(generated_actions)
                        action_log.extend(generated_actions)
                        current = await self._synthesize_codebook(
                            consolidated_codes=current_codes,
                            quote_evidence=quote_evidence,
                            research_query=research_query,
                            researcher_topics=researcher_topics,
                        )

            current = self._ensure_synthesis_covers_codes(current, current_codes)
            after_labels = self._codebook_label_set(current)
            jaccard = self._jaccard_similarity(before_labels, after_labels)
            if jaccard >= cfg.TRACEABLE_REFINEMENT_JACCARD_THRESHOLD:
                action_log.append(
                    {
                        "action": "refinement_stabilized",
                        "round": iteration,
                        "jaccard": jaccard,
                        "threshold": cfg.TRACEABLE_REFINEMENT_JACCARD_THRESHOLD,
                    }
                )
                logger.info(
                    "Traceable iteration refinement stabilized: iteration={}, jaccard={:.3f}",
                    iteration,
                    jaccard,
                )
                break

        if best is None:
            raise UnprocessableError("Traceable iteration loop produced no evaluable codebook.")
        action_log.append(
            {
                "action": "select_best_iteration",
                "selected_iteration": best.iteration,
                "outputs": {"metrics": best.metrics},
            }
        )
        return best, artifacts, action_log

    def _compute_iteration_metrics(
        self,
        *,
        synthesis: CodebookSynthesisResult,
        consolidated_codes: list[ConsolidatedCode],
        quote_evidence: list[_QuoteEvidence],
        evaluation_documents: list[_DocumentText],
        evaluation_evidence: list[_AppliedEvidence],
        used_heldout: bool,
        failed_document_count: int = 0,
    ) -> dict[str, float | int | bool]:
        total_codes = max(1, len(synthesis.codes))
        used_code_keys = {self._label_key(evidence.code_label) for evidence in evaluation_evidence}
        exact_matches = [
            evidence for evidence in evaluation_evidence
            if evidence.quote_match_status == "exact"
        ]
        covered_document_ids = {evidence.document_id for evidence in evaluation_evidence}
        average_confidence = (
            sum(evidence.confidence for evidence in evaluation_evidence) / len(evaluation_evidence)
            if evaluation_evidence
            else 0.0
        )
        train_counts = {
            self._label_key(code.label): code.frequency
            for code in consolidated_codes
        }
        eval_counts: dict[str, int] = defaultdict(int)
        for evidence in evaluation_evidence:
            eval_counts[self._label_key(evidence.code_label)] += 1

        code_reusability = len(used_code_keys) / total_codes
        quote_exact_match_rate = len(exact_matches) / len(evaluation_evidence) if evaluation_evidence else 0.0
        document_coverage = len(covered_document_ids) / len(evaluation_documents) if evaluation_documents else 0.0
        train_eval_consistency = self._cosine_count_similarity(train_counts, eval_counts)
        parsimony_score, target_min, target_max = self._parsimony_score(
            code_count=len(synthesis.codes),
            quote_count=len(quote_evidence),
        )
        overmerge_balance = self._overmerge_balance(consolidated_codes)
        composite = (
            code_reusability
            + quote_exact_match_rate
            + document_coverage
            + average_confidence
            + train_eval_consistency
            + parsimony_score
            + overmerge_balance
        ) / 7
        return {
            "composite_score": composite,
            "code_reusability": code_reusability,
            "quote_exact_match_rate": quote_exact_match_rate,
            "document_coverage": document_coverage,
            "average_assignment_confidence": average_confidence,
            "train_eval_consistency": train_eval_consistency,
            "parsimony_score": parsimony_score,
            "overmerge_balance": overmerge_balance,
            "code_count": len(synthesis.codes),
            "assignment_count": len(evaluation_evidence),
            "failed_document_count": failed_document_count,
            "target_min_codes": target_min,
            "target_max_codes": target_max,
            "used_heldout_evaluation": used_heldout,
        }

    @staticmethod
    def _cosine_count_similarity(left: dict[str, int], right: dict[str, int]) -> float:
        keys = set(left) | set(right)
        if not keys:
            return 0.0
        dot = sum(left.get(key, 0) * right.get(key, 0) for key in keys)
        left_norm = math.sqrt(sum(value * value for value in left.values()))
        right_norm = math.sqrt(sum(value * value for value in right.values()))
        if left_norm == 0.0 or right_norm == 0.0:
            return 0.0
        return dot / (left_norm * right_norm)

    @staticmethod
    def _parsimony_score(*, code_count: int, quote_count: int) -> tuple[float, int, int]:
        target_min = max(5, min(20, int(round(quote_count * 0.18))))
        target_max = max(target_min + 1, min(40, int(round(quote_count * 0.50))))
        if target_min <= code_count <= target_max:
            return 1.0, target_min, target_max
        if code_count < target_min:
            return max(0.0, code_count / target_min), target_min, target_max
        return max(0.0, target_max / max(1, code_count)), target_min, target_max

    @staticmethod
    def _overmerge_balance(consolidated_codes: list[ConsolidatedCode]) -> float:
        if not consolidated_codes:
            return 0.0
        scores = []
        for code in consolidated_codes:
            candidate_count = max(1, len(code.candidate_ids))
            scores.append(1.0 if candidate_count <= 4 else 4 / candidate_count)
        return sum(scores) / len(scores)

    def _apply_code_merge_actions_to_consolidated_codes(
        self,
        consolidated_codes: list[ConsolidatedCode],
        applied_actions: list[dict[str, object]],
    ) -> list[ConsolidatedCode]:
        merge_actions = [
            action for action in applied_actions
            if action.get("applied")
            and action.get("action") == "merge"
            and action.get("artifact_type") == "code"
            and action.get("source_labels")
            and (action.get("replacement") or action.get("target"))
        ]
        current = list(consolidated_codes)
        for action in merge_actions:
            source_keys = {
                self._label_key(str(label))
                for label in action.get("source_labels", [])
            }
            replacement = self._truncate_label(
                self._normalize_label(str(action.get("replacement") or action.get("target") or ""))
            )
            if not source_keys or not replacement:
                continue
            matching = [code for code in current if self._label_key(code.label) in source_keys]
            if not matching:
                continue
            remaining = [code for code in current if self._label_key(code.label) not in source_keys]
            candidate_ids: list[str] = []
            quote_ids: list[str] = []
            descriptions: list[str] = []
            for code in matching:
                candidate_ids.extend(code.candidate_ids)
                for quote_id in code.quote_ids:
                    if quote_id not in quote_ids:
                        quote_ids.append(quote_id)
                if code.description and code.description not in descriptions:
                    descriptions.append(code.description)
            remaining.append(
                ConsolidatedCode(
                    label=replacement,
                    description=self._clean_optional_text(" / ".join(descriptions)),
                    candidate_ids=candidate_ids,
                    quote_ids=quote_ids,
                )
            )
            current = remaining
        return current

    async def _refine_codebook(
        self,
        *,
        synthesis: CodebookSynthesisResult,
        quote_evidence: list[_QuoteEvidence],
        consolidated_codes: list[ConsolidatedCode],
        research_query: str | None,
        researcher_topics: str | None,
        max_rounds: int,
        should_cancel: Callable[[], Awaitable[bool]] | None,
    ) -> tuple[CodebookSynthesisResult, list[ConsolidatedCode], list[dict[str, object]]]:
        action_log: list[dict[str, object]] = []
        current = synthesis
        current_codes = list(consolidated_codes)
        for round_index in range(max(0, max_rounds)):
            await self._raise_if_cancelled(should_cancel)
            before_labels = self._codebook_label_set(current)
            # Reviewer pass: ask for structural edits using a constrained action
            # vocabulary so changes can be logged and applied deterministically.
            review = await self._review_codebook(
                synthesis=current,
                quote_evidence=quote_evidence,
                consolidated_codes=consolidated_codes,
                round_index=round_index + 1,
            )
            if not review.actions:
                action_log.append({"action": "review_complete", "round": round_index + 1, "edits": 0})
                logger.info(
                    "Traceable refinement round complete: round={}, proposed_actions=0, status=no_edits",
                    round_index + 1,
                )
                break
            current, applied_actions = self._apply_review_actions(current, review, round_index=round_index + 1)
            action_log.extend(applied_actions)
            logger.info(
                "Traceable refinement round actions: round={}, proposed_actions={}, applied_actions={}",
                round_index + 1,
                len(review.actions),
                sum(1 for action in applied_actions if action.get("applied")),
            )
            if any(action.action == "generate" for action in review.actions):
                # If the reviewer identifies a genuine missing concept, generate
                # only quote-backed codes from the original evidence payload.
                missing_codes = await self._generate_missing_codes(
                    synthesis=current,
                    quote_evidence=quote_evidence,
                    existing_codes=current_codes,
                    round_index=round_index + 1,
                )
                if missing_codes:
                    existing_keys = {self._label_key(code.label) for code in current_codes}
                    additions = [
                        code for code in missing_codes
                        if self._label_key(code.label) not in existing_keys
                    ]
                    if additions:
                        current_codes.extend(additions)
                        action_log.extend(
                            {
                                "action": "generate_grounded_code",
                                "round": round_index + 1,
                                "target": code.label,
                                "outputs": {"quote_ids": code.quote_ids},
                            }
                            for code in additions
                        )
                        current = await self._synthesize_codebook(
                            consolidated_codes=current_codes,
                            quote_evidence=quote_evidence,
                            research_query=research_query,
                            researcher_topics=researcher_topics,
                        )
                        logger.info(
                            "Traceable refinement generated missing codes: round={}, generated_codes={}",
                            round_index + 1,
                            len(additions),
                        )
            current = self._ensure_synthesis_covers_codes(current, current_codes)
            after_labels = self._codebook_label_set(current)
            threshold = get_settings().TRACEABLE_REFINEMENT_JACCARD_THRESHOLD
            jaccard = self._jaccard_similarity(before_labels, after_labels)
            logger.info(
                "Traceable refinement round summary: round={}, labels_before={}, labels_after={}, "
                "jaccard={:.3f}, threshold={:.3f}",
                round_index + 1,
                len(before_labels),
                len(after_labels),
                jaccard,
                threshold,
            )
            if jaccard >= threshold:
                # Paper-style early stopping: once labels stabilize, further
                # refinement rounds are unlikely to add useful structure.
                action_log.append(
                    {
                        "action": "refinement_stabilized",
                        "round": round_index + 1,
                        "jaccard": jaccard,
                        "threshold": threshold,
                    }
                )
                break
        return current, current_codes, action_log

    async def _generate_missing_codes(
        self,
        *,
        synthesis: CodebookSynthesisResult,
        quote_evidence: list[_QuoteEvidence],
        existing_codes: list[ConsolidatedCode],
        round_index: int,
    ) -> list[ConsolidatedCode]:
        del round_index
        existing_labels = {self._label_key(code.label) for code in existing_codes}
        quote_by_id = {quote.quote_id: quote for quote in quote_evidence}
        evidence_payload = [
            {
                "quote_id": quote.quote_id,
                "quote": quote.quote,
                "initial_code_label": quote.code_label,
            }
            for quote in quote_evidence
        ]
        parser = JsonOutputParser(pydantic_object=MissingCodeGenerationResult)
        chain = build_missing_code_generation_prompt() | build_chat_model(temperature=0.0) | parser
        raw_result = await chain.ainvoke(
            {
                "codebook": json.dumps(synthesis.model_dump(mode="json"), ensure_ascii=True, indent=2),
                "quote_evidence": json.dumps(evidence_payload, ensure_ascii=True, indent=2),
            }
        )
        result = MissingCodeGenerationResult(**raw_result)
        missing: list[ConsolidatedCode] = []
        for item in result.codes:
            label = self._truncate_label(self._normalize_label(item.code_label))
            if not label or self._label_key(label) in existing_labels:
                continue
            quote_ids = [
                quote_id
                for quote_id in item.source_quote_ids
                if quote_id in quote_by_id
            ]
            if not quote_ids:
                continue
            existing_labels.add(self._label_key(label))
            missing.append(
                ConsolidatedCode(
                    label=label,
                    description=self._clean_optional_text(item.code_description),
                    candidate_ids=[f"generated:{self._label_key(label)}"],
                    quote_ids=quote_ids,
                )
            )
        logger.info(
            "Traceable missing-code generation complete: requested_quote_evidence={}, generated_codes={}",
            len(quote_evidence),
            len(missing),
        )
        return missing

    async def _review_codebook(
        self,
        *,
        synthesis: CodebookSynthesisResult,
        quote_evidence: list[_QuoteEvidence],
        consolidated_codes: list[ConsolidatedCode],
        round_index: int,
        metrics: dict[str, float | int | bool] | None = None,
    ) -> CodebookReviewResult:
        quote_count_by_code = self._quote_count_by_code(synthesis, consolidated_codes)
        candidate_count_by_code = {
            self._label_key(code.label): len(code.candidate_ids)
            for code in consolidated_codes
        }
        payload = {
            "round": round_index,
            "metrics": metrics or {},
            "diagnostics": {
                "zero_quote_codes": [
                    code.code_label
                    for code in synthesis.codes
                    if quote_count_by_code.get(self._label_key(code.code_label), 0) == 0
                ],
                "high_merge_risk_codes": [
                    code.code_label
                    for code in synthesis.codes
                    if candidate_count_by_code.get(self._label_key(code.code_label), 0) > 8
                ],
                "target_code_range": [
                    metrics.get("target_min_codes", 0) if metrics else 0,
                    metrics.get("target_max_codes", 0) if metrics else 0,
                ],
            },
            "themes": synthesis.model_dump(mode="json")["themes"],
            "codes": [
                {
                    **code.model_dump(mode="json"),
                    "quote_count": quote_count_by_code.get(self._label_key(code.code_label), 0),
                    "candidate_count": candidate_count_by_code.get(self._label_key(code.code_label), 0),
                }
                for code in synthesis.codes
            ],
            "quote_count": len(quote_evidence),
        }
        parser = JsonOutputParser(pydantic_object=CodebookReviewResult)
        chain = build_codebook_review_prompt() | build_chat_model(temperature=0.0) | parser
        raw_result = await chain.ainvoke({"codebook": json.dumps(payload, ensure_ascii=True, indent=2)})
        return CodebookReviewResult(**raw_result)

    def _apply_review_actions(
        self,
        synthesis: CodebookSynthesisResult,
        review: CodebookReviewResult,
        *,
        round_index: int,
    ) -> tuple[CodebookSynthesisResult, list[dict[str, object]]]:
        # Most reviewer actions are conservative metadata/tree edits. Split is
        # intentionally audit-only because it requires creating new child scopes
        # that should be reviewed by a human or a richer structured prompt.
        current = synthesis
        action_log: list[dict[str, object]] = []
        for action in review.actions:
            before = current.model_dump(mode="json")
            if action.action == "revise":
                current = self._apply_revise_action(current, action.target, action.replacement)
            elif action.action == "merge":
                current = self._apply_merge_action(
                    current,
                    action.source_labels,
                    action.replacement or action.target,
                    action.artifact_type,
                    action.new_parent_path,
                )
            elif action.action == "move":
                current = self._apply_move_action(current, action.target, action.new_parent_path)
            elif action.action == "delete":
                current = self._apply_delete_action(current, action.target)
            elif action.action == "generate":
                current = self._apply_generate_action(current, action.target, action.replacement, action.artifact_type)
            elif action.action == "split":
                # Split requires new child definitions; record it for audit but leave to human review.
                pass
            after = current.model_dump(mode="json")
            action_log.append(
                {
                    "action": action.action,
                    "round": round_index,
                    "target": action.target,
                    "replacement": action.replacement,
                    "source_labels": action.source_labels,
                    "new_parent_path": action.new_parent_path,
                    "artifact_type": action.artifact_type,
                    "reason": action.reason,
                    "applied": before != after,
                }
            )
        return current, action_log

    def _apply_revise_action(
        self,
        synthesis: CodebookSynthesisResult,
        target: str | None,
        replacement: str | None,
    ) -> CodebookSynthesisResult:
        if not target or not replacement:
            return synthesis
        target_key = self._label_key(target)
        replacement = self._truncate_label(self._normalize_label(replacement))
        themes = []
        for theme in synthesis.themes:
            path = []
            for node in theme.path:
                label = replacement if self._label_key(node.label) == target_key else node.label
                path.append(SynthesizedThemeNode(label=label, description=node.description))
            themes.append(SynthesizedThemePath(path=path))
        codes = [
            SynthesizedCode(
                code_label=code.code_label,
                code_description=code.code_description,
                theme_path=[
                    replacement if self._label_key(label) == target_key else label
                    for label in code.theme_path
                ],
            )
            for code in synthesis.codes
        ]
        return CodebookSynthesisResult(themes=themes, codes=codes)

    def _apply_merge_action(
        self,
        synthesis: CodebookSynthesisResult,
        source_labels: list[str],
        replacement: str | None,
        artifact_type: str | None = None,
        new_parent_path: list[str] | None = None,
    ) -> CodebookSynthesisResult:
        if not source_labels or not replacement:
            return synthesis
        replacement = self._truncate_label(self._normalize_label(replacement))
        source_keys = {self._label_key(label) for label in source_labels}
        if artifact_type == "code":
            return self._apply_code_merge_action(
                synthesis,
                source_keys=source_keys,
                replacement=replacement,
                new_parent_path=new_parent_path or [],
            )
        themes = []
        seen_paths: set[tuple[str, ...]] = set()
        for theme in synthesis.themes:
            path = [
                SynthesizedThemeNode(
                    label=replacement if self._label_key(node.label) in source_keys else node.label,
                    description=node.description,
                )
                for node in theme.path
            ]
            key = tuple(node.label for node in path)
            if key in seen_paths:
                continue
            seen_paths.add(key)
            themes.append(SynthesizedThemePath(path=path))
        codes = [
            SynthesizedCode(
                code_label=code.code_label,
                code_description=code.code_description,
                theme_path=[
                    replacement if self._label_key(label) in source_keys else label
                    for label in code.theme_path
                ],
            )
            for code in synthesis.codes
        ]
        return CodebookSynthesisResult(themes=themes, codes=codes)

    def _apply_code_merge_action(
        self,
        synthesis: CodebookSynthesisResult,
        *,
        source_keys: set[str],
        replacement: str,
        new_parent_path: list[str],
    ) -> CodebookSynthesisResult:
        replacement_key = self._label_key(replacement)
        cleaned_parent = [
            self._truncate_label(self._normalize_label(label))
            for label in new_parent_path
            if label.strip()
        ]
        matching_codes = [
            code for code in synthesis.codes
            if self._label_key(code.code_label) in source_keys
        ]
        if not matching_codes:
            return synthesis
        target_path = cleaned_parent or matching_codes[0].theme_path
        descriptions = [
            code.code_description
            for code in matching_codes
            if code.code_description
        ]
        merged_description = self._clean_optional_text(" / ".join(dict.fromkeys(descriptions)))
        codes_by_key: dict[str, SynthesizedCode] = {}
        for code in synthesis.codes:
            key = self._label_key(code.code_label)
            if key in source_keys:
                key = replacement_key
                candidate = SynthesizedCode(
                    code_label=replacement,
                    code_description=merged_description or code.code_description,
                    theme_path=target_path,
                )
            else:
                candidate = code
            existing = codes_by_key.get(key)
            if existing is None:
                codes_by_key[key] = candidate
            elif not existing.code_description and candidate.code_description:
                codes_by_key[key] = existing.model_copy(update={"code_description": candidate.code_description})

        themes = self._dedupe_theme_paths(list(synthesis.themes))
        if target_path:
            existing_paths = {tuple(node.label for node in theme.path) for theme in themes}
            target_tuple = tuple(target_path)
            if target_tuple not in existing_paths:
                themes.append(
                    SynthesizedThemePath(
                        path=[SynthesizedThemeNode(label=label, description=None) for label in target_path]
                    )
                )
        return CodebookSynthesisResult(
            themes=themes,
            codes=list(codes_by_key.values()),
        )

    def _apply_move_action(
        self,
        synthesis: CodebookSynthesisResult,
        target: str | None,
        new_parent_path: list[str],
    ) -> CodebookSynthesisResult:
        if not target or not new_parent_path:
            return synthesis
        target_key = self._label_key(target)
        cleaned_parent = [self._truncate_label(self._normalize_label(label)) for label in new_parent_path if label.strip()]
        if not cleaned_parent:
            return synthesis
        codes = []
        for code in synthesis.codes:
            if self._label_key(code.code_label) == target_key:
                codes.append(code.model_copy(update={"theme_path": cleaned_parent}))
            else:
                codes.append(code)
        themes = list(synthesis.themes)
        if len(cleaned_parent) >= 1:
            theme_path = tuple(cleaned_parent)
            existing_paths = {tuple(node.label for node in theme.path) for theme in themes}
            if theme_path not in existing_paths:
                themes.append(
                    SynthesizedThemePath(
                        path=[SynthesizedThemeNode(label=label, description=None) for label in cleaned_parent]
                    )
                )
        return CodebookSynthesisResult(themes=self._dedupe_theme_paths(themes), codes=codes)

    def _apply_delete_action(
        self,
        synthesis: CodebookSynthesisResult,
        target: str | None,
    ) -> CodebookSynthesisResult:
        if not target:
            return synthesis
        target_key = self._label_key(target)
        codes = [code for code in synthesis.codes if self._label_key(code.code_label) != target_key]
        themes = [
            theme
            for theme in synthesis.themes
            if all(self._label_key(node.label) != target_key for node in theme.path)
        ]
        return CodebookSynthesisResult(themes=self._dedupe_theme_paths(themes), codes=codes)

    def _apply_generate_action(
        self,
        synthesis: CodebookSynthesisResult,
        target: str | None,
        replacement: str | None,
        artifact_type: str | None,
    ) -> CodebookSynthesisResult:
        label = self._truncate_label(self._normalize_label(replacement or target or ""))
        if not label or artifact_type not in {"theme", "subtheme"}:
            return synthesis
        themes = list(synthesis.themes)
        path = [SynthesizedThemeNode(label=label, description=None)]
        if artifact_type == "subtheme" and themes:
            path = [themes[0].path[0], SynthesizedThemeNode(label=label, description=None)]
        themes.append(SynthesizedThemePath(path=path))
        return CodebookSynthesisResult(themes=self._dedupe_theme_paths(themes), codes=list(synthesis.codes))

    @staticmethod
    def _dedupe_theme_paths(themes: list[SynthesizedThemePath]) -> list[SynthesizedThemePath]:
        deduped: list[SynthesizedThemePath] = []
        seen: set[tuple[str, ...]] = set()
        for theme in themes:
            key = tuple(node.label for node in theme.path)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(theme)
        return deduped

    async def _persist_codebook(
        self,
        *,
        codebook_name: str,
        corpus_id: UUID,
        research_query: str | None,
        researcher_topics: str | None,
        synthesis: CodebookSynthesisResult,
    ) -> _PersistedCodebookRefs:
        version = (
            await self._session.scalar(
                select(func.max(Codebook.version)).where(Codebook.corpus_id == corpus_id)
            )
            or 0
        ) + 1
        codebook = Codebook(
            id=uuid.uuid4(),
            corpus_id=corpus_id,
            name=codebook_name,
            description="Generated by experimental traceable analysis.",
            version=version,
            created_by="traceable-analysis",
            research_query=research_query,
            researcher_topics=researcher_topics,
        )
        self._session.add(codebook)
        await self._session.flush()

        theme_by_label: dict[str, Theme] = {}
        edge_keys: set[tuple[str, str]] = set()
        for path in self._all_theme_paths(synthesis):
            parent: Theme | None = None
            for raw_label, raw_description in path:
                label = self._truncate_label(self._normalize_label(raw_label))
                if not label:
                    continue
                theme = theme_by_label.get(self._label_key(label))
                if theme is None:
                    theme = Theme(
                        id=uuid.uuid4(),
                        codebook_id=codebook.id,
                        label=label,
                        description=self._clean_optional_text(raw_description),
                        is_active=True,
                    )
                    self._session.add(theme)
                    await self._session.flush()
                    self._session.add(
                        CodebookThemeRelationship(
                            id=uuid.uuid4(),
                            codebook_id=codebook.id,
                            theme_id=theme.id,
                            is_active=True,
                        )
                    )
                    theme_by_label[self._label_key(label)] = theme
                if parent is not None:
                    edge_key = (self._label_key(parent.label), self._label_key(theme.label))
                    if edge_key not in edge_keys and parent.id != theme.id:
                        edge_keys.add(edge_key)
                        self._session.add(
                            ThemeHierarchyRelationship(
                                id=uuid.uuid4(),
                                codebook_id=codebook.id,
                                parent_theme_id=parent.id,
                                child_theme_id=theme.id,
                                is_active=True,
                            )
                        )
                parent = theme

        code_by_label: dict[str, Code] = {}
        theme_id_by_code_label: dict[str, UUID | None] = {}
        for synthesized_code in synthesis.codes:
            label = self._truncate_label(self._normalize_label(synthesized_code.code_label))
            if not label or self._label_key(label) in code_by_label:
                continue
            code = Code(
                id=uuid.uuid4(),
                codebook_id=codebook.id,
                label=label,
                description=self._clean_optional_text(synthesized_code.code_description),
                is_active=True,
            )
            self._session.add(code)
            await self._session.flush()
            self._session.add(
                CodebookCodeRelationship(
                    id=uuid.uuid4(),
                    codebook_id=codebook.id,
                    code_id=code.id,
                    is_active=True,
                )
            )
            theme = self._deepest_theme_for_path(synthesized_code.theme_path, theme_by_label)
            if theme is not None:
                self._session.add(
                    ThemeCodeRelationship(
                        id=uuid.uuid4(),
                        codebook_id=codebook.id,
                        theme_id=theme.id,
                        code_id=code.id,
                        is_active=True,
                    )
                )
            code_by_label[self._label_key(label)] = code
            theme_id_by_code_label[self._label_key(label)] = theme.id if theme else None

        if not theme_by_label or not code_by_label:
            raise UnprocessableError("Traceable analysis produced an empty codebook.")
        await self._session.commit()
        await self._session.refresh(codebook)
        return _PersistedCodebookRefs(
            codebook=codebook,
            theme_by_label=theme_by_label,
            code_by_label=code_by_label,
            theme_id_by_code_label=theme_id_by_code_label,
        )

    async def _persist_application(
        self,
        *,
        analysis_name: str | None,
        custom_id: str | None,
        corpus_id: UUID,
        documents: list[_DocumentText],
        applied_evidence: list[_AppliedEvidence],
        failed_document_ids: list[UUID],
        persisted: _PersistedCodebookRefs,
    ) -> CodebookApplicationRun:
        run = CodebookApplicationRun(
            id=uuid.uuid4(),
            name=analysis_name,
            custom_id=custom_id,
            corpus_id=corpus_id,
            codebook_id=persisted.codebook.id,
            status="running",
            documents_total=len(documents),
            documents_coded=0,
            documents_failed=0,
            started_at=_utc_now_naive(),
        )
        self._session.add(run)
        await self._session.flush()

        evidence_by_document: dict[UUID, list[_AppliedEvidence]] = defaultdict(list)
        for evidence in applied_evidence:
            evidence_by_document[evidence.document_id].append(evidence)

        failed_ids = set(failed_document_ids)
        coded_documents = 0
        failed_documents = 0
        for document in documents:
            document_evidence = evidence_by_document.get(document.id, [])
            document_failed = document.id in failed_ids
            document_coding = DocumentCoding(
                id=uuid.uuid4(),
                application_run_id=run.id,
                document_id=document.id,
                codebook_id=persisted.codebook.id,
                status="failed" if document_failed else "coded",
                summary=next(
                    (evidence.summary for evidence in document_evidence if evidence.summary),
                    f"Traceable analysis assigned {len(document_evidence)} grounded quote-code pairs.",
                ),
                researcher_notes=next(
                    (evidence.researcher_notes for evidence in document_evidence if evidence.researcher_notes),
                    None,
                ),
                error_message=(
                    "Traceable final application response could not be parsed after retries."
                    if document_failed
                    else None
                ),
            )
            self._session.add(document_coding)
            await self._session.flush()
            if document_failed:
                failed_documents += 1
                continue

            seen_theme_ids: set[UUID] = set()
            for evidence in document_evidence:
                code = persisted.code_by_label.get(self._label_key(evidence.code_label))
                if code is None:
                    continue
                theme_id = persisted.theme_id_by_code_label.get(self._label_key(code.label))
                if evidence.theme_label:
                    theme = persisted.theme_by_label.get(self._label_key(evidence.theme_label))
                    if theme is not None:
                        theme_id = theme.id
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
            coded_documents += 1

        run.status = "succeeded"
        run.documents_coded = coded_documents
        run.documents_failed = failed_documents
        run.finished_at = _utc_now_naive()
        await self._session.commit()
        await self._session.refresh(run)
        return run

    async def _apply_codebook_to_documents(
        self,
        *,
        documents: list[_DocumentText],
        synthesis: CodebookSynthesisResult,
        should_cancel: Callable[[], Awaitable[bool]] | None,
    ) -> _ApplicationPassResult:
        # This is the deductive application pass. It intentionally ignores the
        # initial open-coding assignments and asks the model to use the finalized
        # codebook labels only.
        codebook_context = self._build_application_codebook_context(synthesis)
        parser = JsonOutputParser(pydantic_object=TraceableApplicationResult)
        chain = build_traceable_application_prompt() | build_chat_model(temperature=0.0) | parser
        allowed_codes = {self._label_key(code.code_label): code.code_label for code in synthesis.codes}
        allowed_themes = {
            self._label_key(node.label): node.label
            for theme in synthesis.themes
            for node in theme.path
        }
        applied: list[_AppliedEvidence] = []
        failed_document_ids: list[UUID] = []
        for document in documents:
            await self._raise_if_cancelled(should_cancel)
            result: TraceableApplicationResult | None = None
            payload = {
                "codebook": codebook_context,
                "transcript": document.content,
            }
            for attempt in range(1, _APPLICATION_MAX_ATTEMPTS + 1):
                try:
                    raw_result = await chain.ainvoke(payload)
                    result = TraceableApplicationResult(**raw_result)
                    break
                except Exception as exc:
                    if attempt >= _APPLICATION_MAX_ATTEMPTS:
                        failed_document_ids.append(document.id)
                        logger.warning(
                            "Traceable application document failed after retries: document_id={}, attempts={}, error={}",
                            document.id,
                            attempt,
                            exc,
                        )
                        break
                    logger.warning(
                        "Traceable application document retry: document_id={}, attempt={}, error={}",
                        document.id,
                        attempt,
                        exc,
                    )
                    await asyncio.sleep(0.5 * attempt)
            if result is None:
                continue
            document_assignments_before = len(applied)
            for assignment in result.codes:
                canonical_code = allowed_codes.get(self._label_key(assignment.code_label))
                if canonical_code is None or not assignment.quote.strip():
                    continue
                canonical_theme = None
                if assignment.theme_label:
                    canonical_theme = allowed_themes.get(self._label_key(assignment.theme_label))
                match = locate_quote_span(document.content, assignment.quote)
                applied.append(
                    _AppliedEvidence(
                        document_id=document.id,
                        code_label=canonical_code,
                        theme_label=canonical_theme,
                        quote=match.quote,
                        start_char=match.start_char,
                        end_char=match.end_char,
                        quote_match_status=match.quote_match_status,
                        confidence=self._clamp_confidence(assignment.confidence),
                        rationale=self._clean_optional_text(assignment.rationale),
                        summary=self._clean_optional_text(result.summary),
                        researcher_notes=self._clean_optional_text(result.researcher_notes),
                    )
                )
            logger.info(
                "Traceable application document complete: document_id={}, assignments={}",
                document.id,
                len(applied) - document_assignments_before,
            )
        return _ApplicationPassResult(evidence=applied, failed_document_ids=failed_document_ids)

    @staticmethod
    def _build_application_codebook_context(synthesis: CodebookSynthesisResult) -> str:
        lines = ["Use only the exact theme and code labels listed below.", "", "THEMES AND CODES:"]
        codes_by_path: dict[tuple[str, ...], list[SynthesizedCode]] = defaultdict(list)
        for code in synthesis.codes:
            codes_by_path[tuple(code.theme_path)].append(code)
        for theme in synthesis.themes:
            path = [node.label for node in theme.path]
            lines.append(f"- Theme path: {' > '.join(path)}")
            for node in theme.path:
                if node.description:
                    lines.append(f"  {node.label} definition: {node.description}")
            for code in sorted(codes_by_path.get(tuple(path), []), key=lambda item: item.code_label.lower()):
                lines.append(f"  - Code label: {code.code_label}")
                if code.code_description:
                    lines.append(f"    Code definition: {code.code_description}")
            lines.append("")
        return "\n".join(lines).strip()

    @staticmethod
    def _canonical_code_by_quote_id(consolidated_codes: list[ConsolidatedCode]) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for code in consolidated_codes:
            for quote_id in code.quote_ids:
                mapping[quote_id] = code.label
        return mapping

    def _quote_count_by_code(
        self,
        synthesis: CodebookSynthesisResult,
        consolidated_codes: list[ConsolidatedCode],
    ) -> dict[str, int]:
        frequency_by_key = {
            self._label_key(code.label): code.frequency
            for code in consolidated_codes
        }
        return {
            self._label_key(code.code_label): frequency_by_key.get(self._label_key(code.code_label), 0)
            for code in synthesis.codes
        }

    def _codebook_label_set(self, synthesis: CodebookSynthesisResult) -> set[str]:
        labels = {self._label_key(code.code_label) for code in synthesis.codes}
        labels.update(
            self._label_key(node.label)
            for theme in synthesis.themes
            for node in theme.path
        )
        return labels

    @staticmethod
    def _jaccard_similarity(left: set[str], right: set[str]) -> float:
        if not left and not right:
            return 1.0
        union = left | right
        if not union:
            return 1.0
        return len(left & right) / len(union)

    @staticmethod
    def _all_theme_paths(synthesis: CodebookSynthesisResult) -> list[list[tuple[str, str | None]]]:
        paths: list[list[tuple[str, str | None]]] = []
        for theme in synthesis.themes:
            path = [(node.label, node.description) for node in theme.path if node.label.strip()]
            if path:
                paths.append(path)
        for code in synthesis.codes:
            path = [(label, None) for label in code.theme_path if label.strip()]
            if path:
                paths.append(path)
        if not paths:
            paths.append([("Grounded Findings", "Codes grounded in transcript evidence.")])
        return paths

    def _deepest_theme_for_path(
        self,
        path: list[str],
        theme_by_label: dict[str, Theme],
    ) -> Theme | None:
        for label in reversed(path):
            theme = theme_by_label.get(self._label_key(label))
            if theme is not None:
                return theme
        return next(iter(theme_by_label.values()), None)

    @staticmethod
    async def _raise_if_cancelled(should_cancel: Callable[[], Awaitable[bool]] | None) -> None:
        if should_cancel is not None and await should_cancel():
            raise TraceableAnalysisCancelledError("Traceable analysis was cancelled")

    @staticmethod
    def _build_provenance_payload(
        *,
        quote_evidence: list[_QuoteEvidence],
        consolidated_codes: list[ConsolidatedCode],
        synthesis: CodebookSynthesisResult,
        applied_evidence: list[_AppliedEvidence],
        iteration_artifacts: list[_IterationArtifact] | None = None,
        selected_iteration: int | None = None,
        used_heldout_evaluation: bool = False,
        final_failed_document_ids: list[UUID] | None = None,
    ) -> dict[str, object]:
        # Store paper-like artifact provenance as JSON on the experimental job
        # instead of adding normalized provenance tables during this test phase.
        theme_artifacts: dict[str, dict[str, object]] = {}
        subtheme_artifacts: dict[str, dict[str, object]] = {}
        for theme in synthesis.themes:
            if not theme.path:
                continue
            root = theme.path[0]
            root_id = TraceableAnalysisService._artifact_id("theme", root.label)
            if root_id not in theme_artifacts:
                theme_artifacts[root_id] = {
                    "theme_id": root_id,
                    "label": root.label,
                    "description": root.description,
                    "subtheme_ids": [],
                }
            elif not theme_artifacts[root_id].get("description") and root.description:
                theme_artifacts[root_id]["description"] = root.description
            if len(theme.path) > 1:
                child = theme.path[-1]
                child_id = TraceableAnalysisService._artifact_id("subtheme", child.label)
                if child_id not in subtheme_artifacts:
                    subtheme_artifacts[child_id] = {
                        "subtheme_id": child_id,
                        "label": child.label,
                        "description": child.description,
                        "theme_id": root_id,
                        "code_ids": [],
                    }
                elif not subtheme_artifacts[child_id].get("description") and child.description:
                    subtheme_artifacts[child_id]["description"] = child.description
                subtheme_ids = theme_artifacts[root_id]["subtheme_ids"]
                if isinstance(subtheme_ids, list) and child_id not in subtheme_ids:
                    subtheme_ids.append(child_id)

        code_artifacts = []
        subtheme_id_by_code: dict[str, str | None] = {}
        for code in synthesis.codes:
            code_id = TraceableAnalysisService._artifact_id("code", code.code_label)
            subtheme_id = (
                TraceableAnalysisService._artifact_id("subtheme", code.theme_path[-1])
                if code.theme_path
                else None
            )
            subtheme_id_by_code[TraceableAnalysisService._label_key(code.code_label)] = subtheme_id
            if subtheme_id and subtheme_id in subtheme_artifacts:
                code_ids = subtheme_artifacts[subtheme_id]["code_ids"]
                if isinstance(code_ids, list) and code_id not in code_ids:
                    code_ids.append(code_id)
            source = next(
                (
                    consolidated
                    for consolidated in consolidated_codes
                    if TraceableAnalysisService._label_key(consolidated.label)
                    == TraceableAnalysisService._label_key(code.code_label)
                ),
                None,
            )
            code_artifacts.append(
                {
                    "code_id": code_id,
                    "label": code.code_label,
                    "description": code.code_description,
                    "subtheme_id": subtheme_id,
                    "quote_ids": source.quote_ids if source else [],
                    "candidate_ids": source.candidate_ids if source else [],
                    "frequency": source.frequency if source else 0,
                }
            )
        used_code_keys = {
            TraceableAnalysisService._label_key(evidence.code_label)
            for evidence in applied_evidence
        }
        total_codes = max(1, len(synthesis.codes))
        exact_matches = [
            evidence for evidence in applied_evidence
            if evidence.quote_match_status == "exact"
        ]
        return {
            "metrics": {
                "code_reusability": len(used_code_keys) / total_codes,
                "assignments_total": len(applied_evidence),
                "quote_exact_match_rate": (
                    len(exact_matches) / len(applied_evidence)
                    if applied_evidence
                    else 0.0
                ),
                "selected_iteration": selected_iteration,
                "used_heldout_evaluation": used_heldout_evaluation,
                "final_failed_documents": len(final_failed_document_ids or []),
            },
            "themes": list(theme_artifacts.values()),
            "subthemes": list(subtheme_artifacts.values()),
            "codes": code_artifacts,
            "quotes": [
                {
                    "quote_id": quote.quote_id,
                    "document_id": str(quote.document_id),
                    "candidate_id": quote.candidate_id,
                    "code_label": quote.code_label,
                    "start_char": quote.start_char,
                    "end_char": quote.end_char,
                    "quote_match_status": quote.quote_match_status,
                }
                for quote in quote_evidence
            ],
            "applications": [
                {
                    "document_id": str(evidence.document_id),
                    "code_id": TraceableAnalysisService._artifact_id("code", evidence.code_label),
                    "subtheme_id": subtheme_id_by_code.get(
                        TraceableAnalysisService._label_key(evidence.code_label)
                    ),
                    "quote": evidence.quote,
                    "start_char": evidence.start_char,
                    "end_char": evidence.end_char,
                    "quote_match_status": evidence.quote_match_status,
                }
                for evidence in applied_evidence
            ],
            "synthesis": synthesis.model_dump(mode="json"),
            "iterations": [
                TraceableAnalysisService._iteration_artifact_payload(artifact)
                for artifact in iteration_artifacts or []
            ],
        }

    @staticmethod
    def _iteration_artifact_payload(artifact: _IterationArtifact) -> dict[str, object]:
        return {
            "iteration": artifact.iteration,
            "metrics": artifact.metrics,
            "codes": [
                {
                    "label": code.code_label,
                    "theme_path": code.theme_path,
                }
                for code in artifact.synthesis.codes
            ],
            "themes": artifact.synthesis.model_dump(mode="json")["themes"],
            "evaluation_assignments": len(artifact.evaluation_evidence),
            "actions": artifact.action_log,
        }

    @staticmethod
    def _with_action_ids(action_log: list[dict[str, object]]) -> list[dict[str, object]]:
        enriched = []
        for index, action in enumerate(action_log, start=1):
            action_with_id = {
                "action_id": f"act_{index:04d}",
                "inputs": action.get("inputs", {}),
                "outputs": action.get("outputs", {}),
                **action,
            }
            enriched.append(action_with_id)
        return enriched

    @staticmethod
    def _artifact_id(prefix: str, label: str) -> str:
        key = TraceableAnalysisService._label_key(label).replace(" ", "_")
        return f"{prefix}_{key[:80]}"

    @staticmethod
    def _normalize_label(value: str) -> str:
        return " ".join(value.split()).strip()

    @staticmethod
    def _label_key(value: str) -> str:
        return " ".join(value.lower().split())

    @staticmethod
    def _truncate_label(value: str) -> str:
        return value[:255].strip()

    @staticmethod
    def _clean_optional_text(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = " ".join(value.split()).strip()
        return cleaned or None

    @staticmethod
    def _clamp_confidence(value: float) -> float:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(1.0, numeric))
