from __future__ import annotations

import json
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from langchain_core.output_parsers import JsonOutputParser
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError, UnprocessableError
from app.llm.client import build_chat_model
from app.llm.traceable_prompts import (
    build_code_relationship_prompt,
    build_codebook_review_prompt,
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
    CodebookReviewResult,
    CodebookSynthesisResult,
    CodeRelationshipResult,
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


@dataclass(frozen=True)
class _PersistedCodebookRefs:
    codebook: Codebook
    theme_by_label: dict[str, Theme]
    code_by_label: dict[str, Code]
    theme_id_by_code_label: dict[str, UUID | None]


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class TraceableAnalysisService:
    """Experimental quote-grounded codebook generation plus application."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

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
        on_phase: Callable[[str], Awaitable[None]] | None = None,
        on_codebook_created: Callable[[UUID], Awaitable[None]] | None = None,
        on_application_run_created: Callable[[UUID], Awaitable[None]] | None = None,
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
    ) -> TraceableAnalysisResult:
        normalized_document_ids = self._deduplicate_document_ids(transcript_document_ids)
        await self._load_corpus(corpus_id)
        documents = await self._load_documents(
            corpus_id=corpus_id,
            transcript_document_ids=normalized_document_ids,
        )
        documents = [document for document in documents if document.content.strip()]
        if not documents:
            raise UnprocessableError("No non-empty transcripts found for traceable analysis.")

        await self._session.rollback()

        if on_phase is not None:
            await on_phase("extracting_quote_codes")
        quote_evidence = await self._extract_quote_codes(
            documents=documents,
            research_query=research_query,
            researcher_topics=researcher_topics,
            on_unit_progress=on_unit_progress,
            should_cancel=should_cancel,
        )
        if not quote_evidence:
            raise UnprocessableError("Traceable analysis extracted no grounded quote-code pairs.")

        action_log: list[dict[str, object]] = [
            {
                "action": "extract_quote_code_pairs",
                "documents": len(documents),
                "quotes": len(quote_evidence),
            }
        ]
        candidates = self._build_code_candidates(quote_evidence)
        if on_phase is not None:
            await on_phase("consolidating_codes")
        await self._raise_if_cancelled(should_cancel)
        consolidated_codes, consolidation_log = await consolidate_code_candidates(
            candidates,
            classifier=self._classify_code_pair,
        )
        action_log.extend(consolidation_log)
        if not consolidated_codes:
            raise UnprocessableError("Code consolidation produced no usable codes.")

        if on_phase is not None:
            await on_phase("synthesizing_themes")
        synthesis = await self._synthesize_codebook(
            consolidated_codes=consolidated_codes,
            quote_evidence=quote_evidence,
            research_query=research_query,
            researcher_topics=researcher_topics,
        )
        synthesis = self._ensure_synthesis_covers_codes(synthesis, consolidated_codes)
        action_log.append(
            {
                "action": "synthesize_codebook",
                "themes": len(synthesis.themes),
                "codes": len(synthesis.codes),
            }
        )
        synthesis, review_log = await self._refine_codebook(
            synthesis=synthesis,
            quote_evidence=quote_evidence,
            consolidated_codes=consolidated_codes,
            max_rounds=max_refinement_rounds,
            should_cancel=should_cancel,
        )
        action_log.extend(review_log)

        if on_phase is not None:
            await on_phase("persisting_codebook")
        persisted = await self._persist_codebook(
            codebook_name=codebook_name,
            corpus_id=corpus_id,
            research_query=research_query,
            researcher_topics=researcher_topics,
            synthesis=synthesis,
        )
        if on_codebook_created is not None:
            await on_codebook_created(persisted.codebook.id)

        if on_phase is not None:
            await on_phase("applying_codebook")
        applied_evidence = await self._apply_codebook_to_documents(
            documents=documents,
            synthesis=synthesis,
            should_cancel=should_cancel,
        )
        action_log.append(
            {
                "action": "apply_final_codebook",
                "documents": len(documents),
                "assignments": len(applied_evidence),
            }
        )
        application_run = await self._persist_application(
            analysis_name=analysis_name,
            custom_id=custom_id,
            corpus_id=corpus_id,
            documents=documents,
            applied_evidence=applied_evidence,
            persisted=persisted,
        )
        if on_application_run_created is not None:
            await on_application_run_created(application_run.id)

        provenance = self._build_provenance_payload(
            quote_evidence=quote_evidence,
            consolidated_codes=consolidated_codes,
            synthesis=synthesis,
        )
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
        parser = JsonOutputParser(pydantic_object=CodeRelationshipResult)
        chain = build_code_relationship_prompt() | build_chat_model(temperature=0.0) | parser
        raw_result = await chain.ainvoke(
            {
                "label_a": left.label,
                "description_a": left.description or "",
                "label_b": right.label,
                "description_b": right.description or "",
            }
        )
        return CodeRelationshipResult(**raw_result)

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

    async def _refine_codebook(
        self,
        *,
        synthesis: CodebookSynthesisResult,
        quote_evidence: list[_QuoteEvidence],
        consolidated_codes: list[ConsolidatedCode],
        max_rounds: int,
        should_cancel: Callable[[], Awaitable[bool]] | None,
    ) -> tuple[CodebookSynthesisResult, list[dict[str, object]]]:
        action_log: list[dict[str, object]] = []
        current = synthesis
        for round_index in range(max(0, max_rounds)):
            await self._raise_if_cancelled(should_cancel)
            before_labels = self._codebook_label_set(current)
            review = await self._review_codebook(
                synthesis=current,
                quote_evidence=quote_evidence,
                consolidated_codes=consolidated_codes,
                round_index=round_index + 1,
            )
            if not review.actions:
                action_log.append({"action": "review_complete", "round": round_index + 1, "edits": 0})
                break
            current, applied_actions = self._apply_review_actions(current, review, round_index=round_index + 1)
            action_log.extend(applied_actions)
            current = self._ensure_synthesis_covers_codes(current, consolidated_codes)
            after_labels = self._codebook_label_set(current)
            if self._jaccard_similarity(before_labels, after_labels) >= 0.98:
                action_log.append(
                    {
                        "action": "refinement_stabilized",
                        "round": round_index + 1,
                        "jaccard": self._jaccard_similarity(before_labels, after_labels),
                    }
                )
                break
        return current, action_log

    async def _review_codebook(
        self,
        *,
        synthesis: CodebookSynthesisResult,
        quote_evidence: list[_QuoteEvidence],
        consolidated_codes: list[ConsolidatedCode],
        round_index: int,
    ) -> CodebookReviewResult:
        quote_count_by_code = self._quote_count_by_code(synthesis, consolidated_codes)
        payload = {
            "round": round_index,
            "themes": synthesis.model_dump(mode="json")["themes"],
            "codes": [
                {
                    **code.model_dump(mode="json"),
                    "quote_count": quote_count_by_code.get(self._label_key(code.code_label), 0),
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
        current = synthesis
        action_log: list[dict[str, object]] = []
        for action in review.actions:
            before = current.model_dump(mode="json")
            if action.action == "revise":
                current = self._apply_revise_action(current, action.target, action.replacement)
            elif action.action == "merge":
                current = self._apply_merge_action(current, action.source_labels, action.replacement or action.target)
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
    ) -> CodebookSynthesisResult:
        if not source_labels or not replacement:
            return synthesis
        replacement = self._truncate_label(self._normalize_label(replacement))
        source_keys = {self._label_key(label) for label in source_labels}
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
        return CodebookSynthesisResult(themes=themes, codes=codes)

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
        return CodebookSynthesisResult(themes=themes, codes=codes)

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
        return CodebookSynthesisResult(themes=themes, codes=list(synthesis.codes))

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

        coded_documents = 0
        for document in documents:
            document_evidence = evidence_by_document.get(document.id, [])
            document_coding = DocumentCoding(
                id=uuid.uuid4(),
                application_run_id=run.id,
                document_id=document.id,
                codebook_id=persisted.codebook.id,
                status="coded",
                summary=next(
                    (evidence.summary for evidence in document_evidence if evidence.summary),
                    f"Traceable analysis assigned {len(document_evidence)} grounded quote-code pairs.",
                ),
                researcher_notes=next(
                    (evidence.researcher_notes for evidence in document_evidence if evidence.researcher_notes),
                    None,
                ),
            )
            self._session.add(document_coding)
            await self._session.flush()

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
        run.documents_failed = 0
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
    ) -> list[_AppliedEvidence]:
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
        for document in documents:
            await self._raise_if_cancelled(should_cancel)
            raw_result = await chain.ainvoke(
                {
                    "codebook": codebook_context,
                    "transcript": document.content,
                }
            )
            result = TraceableApplicationResult(**raw_result)
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
        return applied

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
    ) -> dict[str, object]:
        return {
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
            "consolidated_codes": [
                {
                    "label": code.label,
                    "candidate_ids": code.candidate_ids,
                    "quote_ids": code.quote_ids,
                    "frequency": code.frequency,
                }
                for code in consolidated_codes
            ],
            "synthesis": synthesis.model_dump(mode="json"),
        }

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
