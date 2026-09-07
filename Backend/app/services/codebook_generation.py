from __future__ import annotations

import asyncio
import json
import random
import re
import secrets
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from langchain_core.exceptions import OutputParserException
from loguru import logger
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.exceptions import NotFoundError, UnprocessableError
from app.generation.contracts import (
    CodebookDraft,
    GenerationCancelledError,
    GenerationContext,
    GenerationDocument,
    GenerationInput,
    GenerationResult,
    ThemeDraft,
)
from app.generation.loader import (
    GenerationAlgorithmLoadError,
    LoadedGenerationAlgorithm,
    load_generation_algorithm,
)
from app.generation.validation import (
    GenerationDraftValidationError,
    coerce_generation_result,
    validate_generation_result,
)
from app.llm import providers
from app.llm.pipelines import (
    TokenTracker,
    build_codebook_generation_chain,
    consolidate_generated_codes,
    consolidate_generated_themes,
    generate_codebook_for_passages,
)
from app.models import (
    Code,
    Codebook,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Corpus,
    CorpusDocument,
    Theme,
    ThemeCodeRelationship,
    ThemeHierarchyRelationship,
)
from app.schemas.codebook import CodebookSchema, GeneratedCodebookResponse
from app.schemas.llm import (
    CodeConsolidationItem,
    GeneratedThemeNode,
    GeneratedThemePath,
    PassageCodebookGeneration,
)
from app.services.codebook_application import (
    CodebookApplicationCancelledError,
    CodebookApplicationService,
)
from app.services.theme_graph import ThemeGraphService
from app.services.traceable_analysis import (
    TraceableAnalysisCancelledError,
    TraceableAnalysisService,
)

_PASSAGE_GENERATION_MAX_CONCURRENCY = 8
_PASSAGE_GENERATION_BATCH_SIZE = 16
_PASSAGE_GENERATION_MAX_ATTEMPTS = 3
_PASSAGE_GENERATION_RETRY_BASE_DELAY_S = 0.5
_PASSAGE_GENERATION_RETRY_MAX_DELAY_S = 5.0


@dataclass
class _ThemeNodeDraft:
    key: tuple[str, ...]
    label: str
    description: str | None = None


@dataclass
class _CodeDraft:
    label: str
    description: str | None
    parent_theme_key: tuple[str, ...] | None = None


class CodebookGenerationCancelledError(Exception):
    pass


async def resolve_transcript_document_ids(
    session: AsyncSession,
    *,
    corpus_id: UUID,
    transcript_document_ids: list[UUID] | None,
    transcript_sample_size: int | None,
) -> list[UUID] | None:
    """Resolve which transcripts a generation request should use.

    If `transcript_sample_size` is given, randomly picks that many document
    ids from the corpus (unseeded — the story does not require reproducible
    sampling). Otherwise returns `transcript_document_ids` unchanged, so the
    caller's existing "explicit ids, or None/empty for all" behavior applies.
    Called at request time (not inside the generation pipeline) so both the
    sync and job endpoints get the same immediate validation, and the job
    table can persist the concrete resolved ids like it already does for
    explicit selections.
    """
    if not transcript_sample_size:
        return transcript_document_ids

    corpus = (
        await session.execute(select(Corpus.id).where(Corpus.id == corpus_id))
    ).scalar_one_or_none()
    if corpus is None:
        raise NotFoundError(f"Corpus '{corpus_id}' not found")

    available_ids = list(
        (
            await session.scalars(
                select(CorpusDocument.id).where(CorpusDocument.corpus_id == corpus_id)
            )
        ).all()
    )
    if transcript_sample_size > len(available_ids):
        raise UnprocessableError(
            f"Requested transcript_sample_size ({transcript_sample_size}) exceeds the "
            f"number of transcripts available in the corpus ({len(available_ids)})."
        )
    return random.sample(available_ids, transcript_sample_size)


class CodebookGenerationService:
    """Generate and persist a new codebook through a configured strategy."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._llm_tokens_input = 0
        self._llm_tokens_output = 0
        # Kept for compatibility with existing tests and callers that inspect
        # the traceable service token counters directly.
        self.traceable_service = TraceableAnalysisService(self._session)

    @property
    def llm_tokens_input(self) -> int:
        return self._llm_tokens_input

    @property
    def llm_tokens_output(self) -> int:
        return self._llm_tokens_output

    async def generate_codebook(
        self,
        *,
        codebook_name: str,
        corpus_id: UUID,
        transcript_document_ids: list[UUID] | None,
        analysis_name: str | None = None,
        custom_id: str | None = None,
        research_query: str | None = None,
        researcher_topics: str | None = None,
        max_refinement_rounds: int = 5,
        apply_after_generation: bool = True,
        provider: str | None = None,
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
        on_phase_progress: Callable[[str, int, int], Awaitable[None]] | None = None,
        on_phase: Callable[[str], Awaitable[None]] | None = None,
        on_codebook_created: Callable[[UUID], Awaitable[None]] | None = None,
        on_application_run_created: Callable[[UUID], Awaitable[None]] | None = None,
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
        generation_algorithm: str | None = None,
        random_seed: int | None = None,
    ) -> GeneratedCodebookResponse:
        self._llm_tokens_input = 0
        self._llm_tokens_output = 0
        algorithm_spec = generation_algorithm or get_settings().GENERATION_ALGORITHM
        seed = random_seed if random_seed is not None else secrets.randbits(64)
        try:
            loaded_algorithm = load_generation_algorithm(algorithm_spec)
            algorithm = loaded_algorithm.algorithm
            normalized_document_ids = self._deduplicate_document_ids(transcript_document_ids)
            await self._load_corpus(corpus_id)
            documents = await self._load_documents(
                corpus_id=corpus_id,
                transcript_document_ids=normalized_document_ids,
            )
            generation_documents = tuple(
                GenerationDocument(
                    document_id=document.id,
                    title=document.title,
                    content=(document.content or "").strip(),
                )
                for document in documents
                if (document.content or "").strip()
            )
            if not generation_documents:
                raise UnprocessableError("No non-empty transcripts found for codebook generation.")
            await self._session.rollback()

            context = GenerationContext(
                selected_llm_provider=provider,
                llm_model=self._llm_model_for_provider(provider),
                embedding_model=self._embedding_model_for_provider(provider),
                on_progress=on_progress,
                on_phase=on_phase,
                on_phase_progress=on_phase_progress,
                should_cancel=should_cancel,
            )
            generation_input = GenerationInput(
                documents=generation_documents,
                research_query=research_query,
                researcher_topics=researcher_topics,
                random_seed=seed,
                algorithm_options={"max_refinement_rounds": max_refinement_rounds},
            )
            raw_result = await algorithm.generate(generation_input, context)
            generation_result = validate_generation_result(
                coerce_generation_result(raw_result, algorithm_id=algorithm.algorithm_id),
                algorithm_id=algorithm.algorithm_id,
            )
            self._llm_tokens_input = self._token_count(generation_result, "input_tokens")
            self._llm_tokens_output = self._token_count(generation_result, "output_tokens")

            if on_phase is not None:
                await on_phase("persisting_codebook")
            await self._raise_if_cancelled(should_cancel)
            created_codebook, themes_created, codes_created = await self._persist_draft_codebook(
                codebook_name=codebook_name,
                corpus_id=corpus_id,
                research_query=research_query,
                researcher_topics=researcher_topics,
                draft=generation_result.codebook,
                llm_tokens_input=self._llm_tokens_input,
                llm_tokens_output=self._llm_tokens_output,
                algorithm_id=algorithm.algorithm_id,
            )
            created_codebook_id = created_codebook.id
            if on_codebook_created is not None:
                await on_codebook_created(created_codebook_id)

            action_log: list[object] = [dict(action) for action in generation_result.action_log]
            documents_coded = 0
            documents_failed = 0
            application_run_id: UUID | None = None
            if apply_after_generation:
                if on_phase is not None:
                    await on_phase("applying_codebook")

                async def _on_application_progress(
                    done: int,
                    total: int,
                    _coded: int,
                    _failed: int,
                ) -> None:
                    if on_progress is not None:
                        await on_progress(done, total)

                application_service = CodebookApplicationService(self._session)
                application_summary = await application_service.apply_codebook(
                    name=analysis_name or codebook_name,
                    custom_id=custom_id,
                    corpus_id=corpus_id,
                    codebook_id=created_codebook_id,
                    transcript_document_ids=[
                        document.document_id for document in generation_documents
                    ],
                    provider=provider,
                    on_progress=_on_application_progress,
                    on_phase=None,
                    on_run_created=on_application_run_created,
                    should_cancel=should_cancel,
                )
                application_run_id = application_summary.application_run.id
                documents_coded = application_summary.documents_coded
                documents_failed = application_summary.documents_failed
                self._llm_tokens_input += application_service.traceable_service.llm_tokens_input
                self._llm_tokens_output += application_service.traceable_service.llm_tokens_output
                await self._update_generated_token_totals(
                    codebook_id=created_codebook_id,
                    application_run_id=application_run_id,
                    llm_tokens_input=self._llm_tokens_input,
                    llm_tokens_output=self._llm_tokens_output,
                )
                if application_summary.action_log:
                    action_log.extend(application_summary.action_log)
                action_log.append(
                    {
                        "action": "apply_final_codebook",
                        "documents": len(generation_documents),
                        "documents_coded": documents_coded,
                        "documents_failed": documents_failed,
                    }
                )
            else:
                action_log.append(
                    {
                        "action": "skip_final_application",
                        "reason": "apply_after_generation=false",
                    }
                )

            response_codebook = await self._session.get(Codebook, created_codebook_id)
            if response_codebook is None:
                raise NotFoundError("Generated codebook disappeared before response construction")
            provenance = self._enrich_provenance(
                generation_result=generation_result,
                loaded_algorithm=loaded_algorithm,
                random_seed=seed,
                selected_document_ids=[document.document_id for document in generation_documents],
                provider=provider,
                algorithm_options={"max_refinement_rounds": max_refinement_rounds},
                application_run_id=application_run_id,
                documents_coded=documents_coded,
                documents_failed=documents_failed,
            )
            response_action_log = self._with_action_ids(action_log)
            return GeneratedCodebookResponse(
                codebook=CodebookSchema.model_validate(response_codebook),
                application_run_id=application_run_id,
                transcripts_processed=len(generation_documents),
                passages_processed=generation_result.processed_unit_count
                or len(generation_documents),
                themes_created=themes_created,
                codes_created=codes_created,
                documents_coded=documents_coded,
                documents_failed=documents_failed,
                quotes_created=generation_result.quote_count,
                provenance=provenance,
                action_log=response_action_log,
            )
        except GenerationCancelledError as exc:
            raise CodebookGenerationCancelledError("Codebook generation was cancelled") from exc
        except TraceableAnalysisCancelledError as exc:
            raise CodebookGenerationCancelledError("Codebook generation was cancelled") from exc
        except CodebookApplicationCancelledError as exc:
            raise CodebookGenerationCancelledError("Codebook generation was cancelled") from exc
        except (GenerationAlgorithmLoadError, GenerationDraftValidationError) as exc:
            raise UnprocessableError(str(exc)) from exc

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
    ) -> list[CorpusDocument]:
        if not transcript_document_ids:
            return list(
                (
                    await self._session.scalars(
                        select(CorpusDocument)
                        .where(CorpusDocument.corpus_id == corpus_id)
                        .order_by(CorpusDocument.id)
                    )
                ).all()
            )

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
        missing = [
            document_id
            for document_id in transcript_document_ids
            if document_id not in documents_by_id
        ]
        if missing:
            missing_str = ", ".join(str(document_id) for document_id in missing)
            raise UnprocessableError(
                f"Some transcript_document_ids were not found in the selected corpus: {missing_str}"
            )
        return [documents_by_id[document_id] for document_id in transcript_document_ids]

    async def _load_passages(
        self,
        *,
        corpus_id: UUID,
        transcript_document_ids: list[UUID],
    ) -> list[str]:
        docs = list(
            (
                await self._session.scalars(
                    select(CorpusDocument).where(
                        CorpusDocument.corpus_id == corpus_id,
                        CorpusDocument.id.in_(transcript_document_ids),
                    )
                )
            ).all()
        )
        if not docs:
            return []

        docs_by_id = {doc.id: doc for doc in docs}

        passages: list[str] = []
        for document_id in transcript_document_ids:
            doc = docs_by_id.get(document_id)
            if doc and doc.content and doc.content.strip():
                passages.append(doc.content.strip())
        return passages

    async def _generate_per_passage(
        self,
        passages: list[str],
        *,
        research_query: str | None = None,
        researcher_topics: str | None = None,
        provider: str | None = None,
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
        tracker: TokenTracker | None = None,
    ) -> tuple[list[PassageCodebookGeneration], list[GeneratedCodebookResponse.PassageFailure]]:
        started_at = time.monotonic()

        total = len(passages)
        chain = build_codebook_generation_chain(provider=provider)
        completed = 0
        generation_by_index: dict[int, PassageCodebookGeneration] = {}
        parse_failures_by_index: dict[int, Exception] = {}
        attempts_by_index: dict[int, int] = {index: 0 for index in range(total)}
        pending_indexes = list(range(total))

        if on_progress is not None:
            await on_progress(0, total)

        # Retry only the failed subset on each round to avoid reprocessing successful passages.
        while pending_indexes:
            if should_cancel is not None and await should_cancel():
                raise CodebookGenerationCancelledError("Codebook generation was cancelled")
            retryable_failure_detected = False
            retry_indexes: list[int] = []

            for chunk_indexes in self._chunked(pending_indexes, _PASSAGE_GENERATION_BATCH_SIZE):
                if should_cancel is not None and await should_cancel():
                    raise CodebookGenerationCancelledError("Codebook generation was cancelled")
                # Use LangChain default async batching and keep strict index mapping.
                batch_results = await generate_codebook_for_passages(
                    [passages[index] for index in chunk_indexes],
                    chain=chain,
                    max_concurrency=_PASSAGE_GENERATION_MAX_CONCURRENCY,
                    research_query=research_query,
                    researcher_topics=researcher_topics,
                    tracker=tracker,
                )
                for local_index, result in enumerate(batch_results):
                    passage_index = chunk_indexes[local_index]
                    attempts_by_index[passage_index] += 1
                    attempt_count = attempts_by_index[passage_index]

                    if isinstance(result, PassageCodebookGeneration):
                        generation_by_index[passage_index] = result
                        completed += 1
                        continue

                    if isinstance(result, (OutputParserException, ValidationError)):
                        if attempt_count < _PASSAGE_GENERATION_MAX_ATTEMPTS:
                            retry_indexes.append(passage_index)
                        else:
                            parse_failures_by_index[passage_index] = result
                            completed += 1
                        continue

                    # Retry transient provider/network failures, but fail fast on hard errors.
                    if (
                        self._is_retryable_llm_exception(result)
                        and attempt_count < _PASSAGE_GENERATION_MAX_ATTEMPTS
                    ):
                        retryable_failure_detected = True
                        retry_indexes.append(passage_index)
                        continue

                    raise UnprocessableError(f"Codebook generation failed: {result}") from result

                if on_progress is not None:
                    await on_progress(completed, total)

            if not retry_indexes:
                break
            pending_indexes = retry_indexes
            if retryable_failure_detected:
                retry_attempt = max(attempts_by_index[index] for index in retry_indexes)
                retry_delay = self._compute_retry_delay(
                    attempt=retry_attempt,
                    base_delay_s=_PASSAGE_GENERATION_RETRY_BASE_DELAY_S,
                    max_delay_s=_PASSAGE_GENERATION_RETRY_MAX_DELAY_S,
                )
                if retry_delay > 0:
                    await asyncio.sleep(retry_delay)

        results = [generation_by_index[index] for index in sorted(generation_by_index)]
        failures = [
            GeneratedCodebookResponse.PassageFailure(
                passage_index=index + 1,
                passage_excerpt=passages[index][:240],
                error=str(parse_failures_by_index[index]),
                attempts=attempts_by_index[index],
            )
            for index in sorted(parse_failures_by_index)
        ]
        logger.info(
            "Passage generation complete: passages={}, succeeded={}, failed={}, total_attempts={}, "
            "max_concurrency={}, batch_size={}, elapsed_s={:.2f}",
            total,
            len(results),
            len(failures),
            sum(attempts_by_index.values()),
            _PASSAGE_GENERATION_MAX_CONCURRENCY,
            _PASSAGE_GENERATION_BATCH_SIZE,
            time.monotonic() - started_at,
        )
        return results, failures

    @staticmethod
    def _chunked(items: list[int], chunk_size: int) -> list[list[int]]:
        return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]

    @staticmethod
    def _is_retryable_llm_exception(exc: Exception) -> bool:
        if isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError, OSError)):
            return True
        message = str(exc).lower()
        retryable_markers = (
            "429",
            "502",
            "503",
            "504",
            "rate limit",
            "too many requests",
            "timeout",
            "timed out",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "service unavailable",
        )
        return any(marker in message for marker in retryable_markers)

    @staticmethod
    def _compute_retry_delay(
        *,
        attempt: int,
        base_delay_s: float,
        max_delay_s: float,
    ) -> float:
        if base_delay_s <= 0:
            return 0.0
        capped_attempt = max(1, attempt)
        backoff = base_delay_s * (2 ** (capped_attempt - 1))
        jitter: float = random.uniform(0.8, 1.2)
        return float(min(max_delay_s, backoff * jitter))

    @staticmethod
    def _normalize_label(value: str) -> str:
        return " ".join(value.split()).strip()

    @staticmethod
    async def _raise_if_cancelled(
        should_cancel: Callable[[], Awaitable[bool]] | None,
    ) -> None:
        if should_cancel is not None and await should_cancel():
            raise CodebookGenerationCancelledError("Codebook generation was cancelled")

    async def _post_process_codes(
        self,
        codes: list[_CodeDraft],
        *,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
        tracker: TokenTracker | None = None,
    ) -> list[_CodeDraft]:
        """Consolidate generated codes and keep a deterministic fallback."""
        await self._raise_if_cancelled(should_cancel)
        if not codes:
            return []
        if len(codes) == 1:
            # No overlap resolution needed for a single code.
            return codes

        consolidation_payload = [
            CodeConsolidationItem(
                label=code.label,
                description=code.description,
                theme_path=self._theme_path_for_key(code.parent_theme_key, theme_nodes=theme_nodes),
            )
            for code in codes
        ]
        original_labels = [code.label for code in codes]
        parent_theme_key_by_label = {
            code.label.lower(): code.parent_theme_key
            for code in codes
            if code.parent_theme_key is not None
        }
        # Consolidation can rename or merge codes; this keeps merged codes
        # attached to a reasonable theme even when the LLM omits the path.
        fallback_parent_theme_key = next(
            (code.parent_theme_key for code in codes if code.parent_theme_key is not None),
            None,
        )
        try:
            consolidated = await asyncio.to_thread(
                consolidate_generated_codes,
                consolidation_payload,
                tracker=tracker,
            )
            await self._raise_if_cancelled(should_cancel)
        except CodebookGenerationCancelledError:
            raise
        except Exception:
            # Keep raw deduplicated codes if consolidation fails for any reason.
            logger.exception(
                "Code consolidation failed; using pre-consolidation code list (count={count})",
                count=len(codes),
            )
            return codes

        consolidated_codes: list[_CodeDraft] = []
        seen_labels: set[str] = set()
        for code in consolidated.codes:
            normalized_label = self._normalize_label(code.label)
            if not normalized_label:
                continue
            code_key = normalized_label.lower()
            if code_key in seen_labels:
                continue
            seen_labels.add(code_key)
            description = code.description.strip() if code.description else None
            returned_theme_key = self._theme_key_from_path(code.theme_path)
            # Prefer the LLM-returned path, then the original code's path, then
            # any known path so consolidated codes stay reachable.
            consolidated_codes.append(
                _CodeDraft(
                    label=normalized_label,
                    description=description or None,
                    parent_theme_key=(
                        returned_theme_key
                        or parent_theme_key_by_label.get(code_key)
                        or fallback_parent_theme_key
                    ),
                )
            )

        if not consolidated_codes:
            logger.warning(
                "Code consolidation returned no usable codes; using pre-consolidation list (count={count})",
                count=len(codes),
            )
            return codes

        consolidated_labels = [code.label for code in consolidated_codes]
        kept_label_keys = {label.lower() for label in consolidated_labels}
        removed_labels = sorted(
            [label for label in original_labels if label.lower() not in kept_label_keys]
        )
        logger.info(
            "Code consolidation finished: before={before}, after={after}, removed={removed}",
            before=len(original_labels),
            after=len(consolidated_labels),
            removed=len(removed_labels),
        )
        logger.debug("Code consolidation kept labels: {}", consolidated_labels)
        logger.debug("Code consolidation removed labels: {}", removed_labels)
        return consolidated_codes

    @staticmethod
    def _theme_key_from_path(
        theme_path: list[str] | tuple[str, ...] | None,
    ) -> tuple[str, ...] | None:
        if not theme_path:
            return None
        normalized = [
            CodebookGenerationService._normalize_label(path_item).lower()
            for path_item in theme_path
            if CodebookGenerationService._normalize_label(path_item)
        ]
        return tuple(normalized) if normalized else None

    @staticmethod
    def _theme_path_for_key(
        parent_theme_key: tuple[str, ...] | None,
        *,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
    ) -> list[str]:
        if parent_theme_key is None:
            return []
        path: list[str] = []
        for index in range(1, len(parent_theme_key) + 1):
            path_key = parent_theme_key[:index]
            node = theme_nodes.get(path_key)
            path.append(node.label if node is not None else parent_theme_key[index - 1])
        return path

    @classmethod
    def _remap_code_parent_keys(
        cls,
        codes: list[_CodeDraft],
        *,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
    ) -> list[_CodeDraft]:
        """Ensure every code parent key points at a theme in the final tree."""
        if not codes or not theme_nodes:
            return codes

        candidate_keys = cls._candidate_theme_keys(theme_nodes)
        leaf_key_by_label = {theme_nodes[key].label.lower(): key for key in candidate_keys}

        remapped: list[_CodeDraft] = []
        for code in codes:
            parent_key = code.parent_theme_key
            if parent_key in theme_nodes:
                resolved_key: tuple[str, ...] | None = parent_key
            elif parent_key and parent_key[-1] in leaf_key_by_label:
                # Theme consolidation may move a leaf under a different parent.
                resolved_key = leaf_key_by_label[parent_key[-1]]
                logger.warning(
                    "Remapped generated code to final theme by leaf label: code_label={code_label!r}, "
                    "original_parent_key={original_parent_key}, resolved_parent_key={resolved_parent_key}",
                    code_label=code.label,
                    original_parent_key=parent_key,
                    resolved_parent_key=resolved_key,
                )
            else:
                # Last resort: use token overlap between the code and final
                # theme labels to avoid dropping useful code-theme links.
                resolved_key = cls._best_matching_theme_key(
                    code=code,
                    theme_nodes=theme_nodes,
                    candidate_keys=candidate_keys,
                )
                if resolved_key is not None:
                    logger.warning(
                        "Remapped generated code to final theme by token fallback: code_label={code_label!r}, "
                        "original_parent_key={original_parent_key}, resolved_parent_key={resolved_parent_key}",
                        code_label=code.label,
                        original_parent_key=parent_key,
                        resolved_parent_key=resolved_key,
                    )
                else:
                    logger.warning(
                        "Generated code could not be mapped to a final theme and will remain unattached: "
                        "code_label={code_label!r}, original_parent_key={original_parent_key}",
                        code_label=code.label,
                        original_parent_key=parent_key,
                    )
            remapped.append(
                _CodeDraft(
                    label=code.label,
                    description=code.description,
                    parent_theme_key=resolved_key,
                )
            )
        return remapped

    @classmethod
    def _best_matching_theme_key(
        cls,
        *,
        code: _CodeDraft,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
        candidate_keys: list[tuple[str, ...]],
    ) -> tuple[str, ...] | None:
        if not candidate_keys:
            return None

        source_labels = [code.label]
        if code.parent_theme_key:
            source_labels.extend(code.parent_theme_key)
        source_text = " ".join(source_labels)
        source_tokens = cls._label_tokens(source_text)
        if not source_tokens:
            return None

        best_key: tuple[str, ...] | None = None
        best_score = 0
        for candidate_key in candidate_keys:
            candidate_path = [
                theme_nodes[path_key].label
                for index in range(1, len(candidate_key) + 1)
                if (path_key := candidate_key[:index]) in theme_nodes
            ]
            candidate_tokens = cls._label_tokens(" ".join(candidate_path))
            score = cls._token_overlap_score(source_tokens, candidate_tokens)
            if score > best_score:
                best_score = score
                best_key = candidate_key

        return best_key if best_score > 0 else None

    @staticmethod
    def _candidate_theme_keys(
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
    ) -> list[tuple[str, ...]]:
        parent_keys = {key[:-1] for key in theme_nodes if len(key) > 1}
        leaf_keys = [key for key in theme_nodes if key not in parent_keys]
        root_keys = [key for key in theme_nodes if len(key) == 1]
        # Prefer leaves because codes usually point to the most specific theme.
        return sorted(
            [*leaf_keys, *[key for key in root_keys if key not in leaf_keys]],
            key=lambda key: (len(key), key),
        )

    @staticmethod
    def _label_tokens(value: str) -> set[str]:
        return {token for token in re.findall(r"[a-z0-9]+", value.lower()) if len(token) >= 3}

    @staticmethod
    def _token_overlap_score(source_tokens: set[str], candidate_tokens: set[str]) -> int:
        score = 0
        for source in source_tokens:
            for candidate in candidate_tokens:
                if source == candidate:
                    score += 3
                elif (
                    len(source) >= 5
                    and len(candidate) >= 5
                    and (source in candidate or candidate in source)
                ):
                    score += 1
        return score

    async def _post_process_themes(
        self,
        *,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
        hierarchy_edges: list[tuple[tuple[str, ...], tuple[str, ...]]],
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
        tracker: TokenTracker | None = None,
    ) -> tuple[
        dict[tuple[str, ...], _ThemeNodeDraft], list[tuple[tuple[str, ...], tuple[str, ...]]]
    ]:
        """Consolidate theme paths and rebuild the theme tree from consolidated paths."""
        await self._raise_if_cancelled(should_cancel)
        if not theme_nodes:
            return theme_nodes, hierarchy_edges

        theme_paths = self._theme_paths_from_graph(
            theme_nodes=theme_nodes,
            hierarchy_edges=hierarchy_edges,
        )
        if len(theme_paths) <= 1:
            return theme_nodes, hierarchy_edges

        # Compress noisy passage-level themes while keeping enough structure for
        # a useful codebook.
        target_total_themes = min(40, max(20, int(round(len(theme_nodes) * 0.35))))
        first_pass_constraints = self._build_theme_consolidation_constraints(
            max_root_themes=10,
            target_total_themes=target_total_themes,
            aggressive=False,
        )
        try:
            consolidated = await asyncio.to_thread(
                consolidate_generated_themes,
                theme_paths,
                constraints=first_pass_constraints,
                tracker=tracker,
            )
            await self._raise_if_cancelled(should_cancel)
        except CodebookGenerationCancelledError:
            raise
        except Exception:
            logger.exception(
                "Theme consolidation failed; using pre-consolidation theme tree (themes={count}, paths={paths})",
                count=len(theme_nodes),
                paths=len(theme_paths),
            )
            return theme_nodes, hierarchy_edges

        consolidated_theme_nodes, consolidated_edges = self._build_theme_graph_from_paths(
            consolidated.themes
        )
        if not consolidated_theme_nodes:
            logger.warning(
                "Theme consolidation returned no usable themes; using pre-consolidation tree (themes={count})",
                count=len(theme_nodes),
            )
            return theme_nodes, hierarchy_edges

        # If first pass remains too broad, run a stricter compression pass.
        consolidated_root_count = self._count_root_themes(
            consolidated_edges, consolidated_theme_nodes
        )
        if consolidated_root_count > 10 or len(consolidated_theme_nodes) > target_total_themes:
            strict_constraints = self._build_theme_consolidation_constraints(
                max_root_themes=8,
                target_total_themes=min(target_total_themes, 30),
                aggressive=True,
            )
            try:
                strict_consolidated = await asyncio.to_thread(
                    consolidate_generated_themes,
                    consolidated.themes,
                    constraints=strict_constraints,
                    tracker=tracker,
                )
                await self._raise_if_cancelled(should_cancel)
                strict_nodes, strict_edges = self._build_theme_graph_from_paths(
                    strict_consolidated.themes
                )
                if strict_nodes:
                    consolidated = strict_consolidated
                    consolidated_theme_nodes = strict_nodes
                    consolidated_edges = strict_edges
            except CodebookGenerationCancelledError:
                raise
            except Exception:
                logger.exception(
                    "Strict theme consolidation pass failed; using first-pass consolidated tree"
                )

        original_labels = sorted({node.label for node in theme_nodes.values()})
        consolidated_labels = sorted({node.label for node in consolidated_theme_nodes.values()})
        kept_label_keys = {label.lower() for label in consolidated_labels}
        removed_labels = sorted(
            [label for label in original_labels if label.lower() not in kept_label_keys]
        )

        logger.info(
            "Theme consolidation finished: before_themes={before_themes}, after_themes={after_themes}, "
            "before_paths={before_paths}, after_paths={after_paths}, removed_labels={removed}",
            before_themes=len(theme_nodes),
            after_themes=len(consolidated_theme_nodes),
            before_paths=len(theme_paths),
            after_paths=len(consolidated.themes),
            removed=len(removed_labels),
        )
        logger.debug("Theme consolidation kept labels: {}", consolidated_labels)
        logger.debug("Theme consolidation removed labels: {}", removed_labels)
        logger.debug(
            "Theme consolidation output paths: {}",
            [
                " > ".join(
                    self._normalize_label(path_node.label)
                    for path_node in theme_path.path
                    if self._normalize_label(path_node.label)
                )
                for theme_path in consolidated.themes
            ],
        )
        return consolidated_theme_nodes, consolidated_edges

    @staticmethod
    def _count_root_themes(
        hierarchy_edges: list[tuple[tuple[str, ...], tuple[str, ...]]],
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
    ) -> int:
        children = {child for _, child in hierarchy_edges}
        return len([key for key in theme_nodes if key not in children])

    @staticmethod
    def _build_theme_consolidation_constraints(
        *,
        max_root_themes: int,
        target_total_themes: int,
        aggressive: bool,
    ) -> str:
        extra = (
            (
                "- Be highly aggressive: collapse near-duplicates and subordinate variants unless analytically necessary.\n"
                "- Do not keep narrow examples (specific jobs, incidents, or anecdotes) as Level-1 or Level-2 themes.\n"
            )
            if aggressive
            else ""
        )
        return (
            "- Use 3 conceptual levels whenever possible:\n"
            "  1) Domain-level themes (Level-1 roots).\n"
            "  2) Analytical themes (Level-2).\n"
            "  3) Granular subthemes (Level-3+) only for recurring dimensions.\n"
            f"- Keep Level-1 roots at <= {max_root_themes} and prefer 6-10.\n"
            f"- Keep total themes across all levels near {target_total_themes}.\n"
            "- Parent-child compatibility rule: child must be a type, cause, consequence, example, or dimension "
            "of parent.\n"
            "- If a label is an anecdotal detail or one-off example, move it down or drop it.\n"
            f"{extra}"
        )

    @classmethod
    def _theme_paths_from_graph(
        cls,
        *,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
        hierarchy_edges: list[tuple[tuple[str, ...], tuple[str, ...]]],
    ) -> list[GeneratedThemePath]:
        child_to_parent: dict[tuple[str, ...], tuple[str, ...]] = {}
        children_by_parent: dict[tuple[str, ...], list[tuple[str, ...]]] = {}
        for parent, child in hierarchy_edges:
            child_to_parent[child] = parent
            children_by_parent.setdefault(parent, []).append(child)

        for children in children_by_parent.values():
            children.sort(key=lambda key: (len(key), key))

        roots = sorted(
            [key for key in theme_nodes if key not in child_to_parent],
            key=lambda key: (len(key), key),
        )
        paths: list[GeneratedThemePath] = []

        def walk(current: tuple[str, ...], stack: list[tuple[str, ...]]) -> None:
            next_stack = [*stack, current]
            children = children_by_parent.get(current, [])
            if not children:
                # The consolidation prompt expects full root-to-leaf paths.
                paths.append(
                    GeneratedThemePath(
                        path=[
                            GeneratedThemeNode(
                                label=theme_nodes[node_key].label,
                                description=theme_nodes[node_key].description,
                            )
                            for node_key in next_stack
                        ]
                    )
                )
                return
            for child in children:
                walk(child, next_stack)

        for root in roots:
            walk(root, [])

        return paths

    @classmethod
    def _build_theme_graph_from_paths(
        cls,
        theme_paths: list[GeneratedThemePath],
    ) -> tuple[
        dict[tuple[str, ...], _ThemeNodeDraft], list[tuple[tuple[str, ...], tuple[str, ...]]]
    ]:
        theme_nodes_by_key: dict[tuple[str, ...], _ThemeNodeDraft] = {}
        raw_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

        for theme_path in theme_paths:
            normalized_labels = [cls._normalize_label(node.label) for node in theme_path.path]
            normalized_labels = [label for label in normalized_labels if label]
            if not normalized_labels:
                continue

            for index, label in enumerate(normalized_labels, start=1):
                key = tuple(part.lower() for part in normalized_labels[:index])
                description = theme_path.path[index - 1].description
                existing = theme_nodes_by_key.get(key)
                if existing is None:
                    theme_nodes_by_key[key] = _ThemeNodeDraft(
                        key=key,
                        label=label,
                        description=description.strip() if description else None,
                    )
                elif not existing.description and description and description.strip():
                    existing.description = description.strip()
                if index > 1:
                    raw_edges.append(
                        (tuple(part.lower() for part in normalized_labels[: index - 1]), key)
                    )

        # Merge identical labels across different paths; the resulting graph
        # must have one canonical node per label to avoid duplicate themes.
        canonical_key_by_label: dict[str, tuple[str, ...]] = {}
        for key in sorted(theme_nodes_by_key.keys(), key=lambda item: (len(item), item)):
            label_key = theme_nodes_by_key[key].label.lower()
            canonical_key_by_label.setdefault(label_key, key)

        canonical_theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft] = {}
        canonical_key_by_original: dict[tuple[str, ...], tuple[str, ...]] = {}
        for key, node in theme_nodes_by_key.items():
            canonical_key = canonical_key_by_label[node.label.lower()]
            canonical_key_by_original[key] = canonical_key
            canonical_node = canonical_theme_nodes.get(canonical_key)
            if canonical_node is None:
                canonical_theme_nodes[canonical_key] = _ThemeNodeDraft(
                    key=canonical_key,
                    label=theme_nodes_by_key[canonical_key].label,
                    description=node.description,
                )
            elif not canonical_node.description and node.description:
                canonical_node.description = node.description

        canonical_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
        child_parent: dict[tuple[str, ...], tuple[str, ...]] = {}
        seen_edges: set[tuple[tuple[str, ...], tuple[str, ...]]] = set()
        for parent, child in sorted(
            raw_edges, key=lambda pair: (len(pair[0]), pair[0], len(pair[1]), pair[1])
        ):
            canonical_parent = canonical_key_by_original.get(parent)
            canonical_child = canonical_key_by_original.get(child)
            if canonical_parent is None or canonical_child is None:
                continue
            if canonical_parent == canonical_child:
                continue
            existing_parent = child_parent.get(canonical_child)
            if existing_parent is not None and existing_parent != canonical_parent:
                # Keep a tree shape after label merging by allowing one parent.
                continue
            edge = (canonical_parent, canonical_child)
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            child_parent[canonical_child] = canonical_parent
            canonical_edges.append(edge)

        return canonical_theme_nodes, canonical_edges

    @classmethod
    def _deduplicate_generation(
        cls,
        generation_results: list[PassageCodebookGeneration],
    ) -> tuple[
        dict[tuple[str, ...], _ThemeNodeDraft],
        list[_CodeDraft],
        list[tuple[tuple[str, ...], tuple[str, ...]]],
    ]:
        theme_nodes_by_key: dict[tuple[str, ...], _ThemeNodeDraft] = {}
        codes_by_key: dict[str, _CodeDraft] = {}
        raw_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

        for result in generation_results:
            for generated_path in result.themes:
                normalized_labels = [
                    cls._normalize_label(node.label) for node in generated_path.path
                ]
                normalized_labels = [label for label in normalized_labels if label]
                if not normalized_labels:
                    continue

                for index, label in enumerate(normalized_labels, start=1):
                    key = tuple(part.lower() for part in normalized_labels[:index])
                    description = generated_path.path[index - 1].description
                    existing = theme_nodes_by_key.get(key)
                    if existing is None:
                        theme_nodes_by_key[key] = _ThemeNodeDraft(
                            key=key,
                            label=label,
                            description=description.strip() if description else None,
                        )
                    elif not existing.description and description and description.strip():
                        existing.description = description.strip()
                    if index > 1:
                        raw_edges.append(
                            (tuple(part.lower() for part in normalized_labels[: index - 1]), key)
                        )

            for generated_code in result.codes:
                normalized_code_label = cls._normalize_label(generated_code.label)
                normalized_theme_path = [
                    cls._normalize_label(path_item) for path_item in generated_code.theme_path
                ]
                normalized_theme_path = [item for item in normalized_theme_path if item]
                if not normalized_code_label or not normalized_theme_path:
                    continue

                theme_key: tuple[str, ...] = ()
                for index, label in enumerate(normalized_theme_path, start=1):
                    theme_key = tuple(part.lower() for part in normalized_theme_path[:index])
                    if theme_key not in theme_nodes_by_key:
                        # Codes can reference a theme path that was missing from
                        # the theme list; create that path so the link survives.
                        theme_nodes_by_key[theme_key] = _ThemeNodeDraft(
                            key=theme_key,
                            label=label,
                            description=None,
                        )

                code_key = normalized_code_label.lower()
                existing_code = codes_by_key.get(code_key)
                if existing_code is None:
                    codes_by_key[code_key] = _CodeDraft(
                        label=normalized_code_label,
                        description=generated_code.description.strip()
                        if generated_code.description
                        else None,
                        parent_theme_key=theme_key if theme_key else None,
                    )
                elif not existing_code.description and generated_code.description:
                    description = generated_code.description.strip()
                    if description:
                        existing_code.description = description

        sorted_codes = sorted(
            codes_by_key.values(),
            key=lambda code: code.label.lower(),
        )
        # Canonicalize duplicate theme labels before persistence so hierarchy
        # edges and code links point to stable theme keys.
        canonical_key_by_label: dict[str, tuple[str, ...]] = {}
        for key in sorted(theme_nodes_by_key.keys(), key=lambda item: (len(item), item)):
            label_key = theme_nodes_by_key[key].label.lower()
            canonical_key_by_label.setdefault(label_key, key)

        canonical_theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft] = {}
        canonical_key_by_original: dict[tuple[str, ...], tuple[str, ...]] = {}
        for key, node in theme_nodes_by_key.items():
            canonical_key = canonical_key_by_label[node.label.lower()]
            canonical_key_by_original[key] = canonical_key
            canonical_node = canonical_theme_nodes.get(canonical_key)
            if canonical_node is None:
                canonical_theme_nodes[canonical_key] = _ThemeNodeDraft(
                    key=canonical_key,
                    label=theme_nodes_by_key[canonical_key].label,
                    description=node.description,
                )
            elif not canonical_node.description and node.description:
                canonical_node.description = node.description

        canonical_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
        child_parent: dict[tuple[str, ...], tuple[str, ...]] = {}
        seen_edges: set[tuple[tuple[str, ...], tuple[str, ...]]] = set()
        for parent, child in sorted(
            raw_edges, key=lambda pair: (len(pair[0]), pair[0], len(pair[1]), pair[1])
        ):
            canonical_parent = canonical_key_by_original.get(parent)
            canonical_child = canonical_key_by_original.get(child)
            if canonical_parent is None or canonical_child is None:
                continue
            if canonical_parent == canonical_child:
                continue
            existing_parent = child_parent.get(canonical_child)
            if existing_parent is not None and existing_parent != canonical_parent:
                # Keep the earliest deterministic parent when merged labels
                # create competing parent candidates.
                continue
            edge = (canonical_parent, canonical_child)
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            child_parent[canonical_child] = canonical_parent
            canonical_edges.append(edge)

        return canonical_theme_nodes, sorted_codes, canonical_edges

    async def _persist_draft_codebook(
        self,
        *,
        codebook_name: str,
        corpus_id: UUID,
        research_query: str | None,
        researcher_topics: str | None,
        draft: CodebookDraft,
        llm_tokens_input: int | None,
        llm_tokens_output: int | None,
        algorithm_id: str,
    ) -> tuple[Codebook, int, int]:
        try:
            version = await self._next_codebook_version(corpus_id=corpus_id)
            codebook = Codebook(
                id=uuid.uuid4(),
                corpus_id=corpus_id,
                name=codebook_name,
                description=f"Generated by {algorithm_id}.",
                version=version,
                created_by="system-llm",
                research_query=research_query,
                researcher_topics=researcher_topics,
                llm_tokens_input=llm_tokens_input,
                llm_tokens_output=llm_tokens_output,
            )
            self._session.add(codebook)
            await self._session.flush()

            themes_by_key: dict[str, ThemeDraft] = {}
            for theme in draft.themes:
                theme_key = self._draft_key(theme.key)
                if theme_key is not None:
                    themes_by_key[theme_key] = theme
            theme_id_by_key: dict[str, UUID] = {}
            for theme_key in sorted(
                themes_by_key, key=lambda key: (self._theme_depth(key, themes_by_key), key)
            ):
                theme = themes_by_key[theme_key]
                theme_row = Theme(
                    id=uuid.uuid4(),
                    codebook_id=codebook.id,
                    label=self._truncate_label(self._normalize_label(theme.label)),
                    description=self._clean_optional_text(theme.description),
                    is_active=True,
                )
                self._session.add(theme_row)
                await self._session.flush()
                theme_id_by_key[theme_key] = theme_row.id
                self._session.add(
                    CodebookThemeRelationship(
                        id=uuid.uuid4(),
                        codebook_id=codebook.id,
                        theme_id=theme_row.id,
                        is_active=True,
                    )
                )

            added_edges: set[tuple[UUID, UUID]] = set()
            for theme_key in sorted(
                themes_by_key, key=lambda key: (self._theme_depth(key, themes_by_key), key)
            ):
                parent_key = self._draft_key(themes_by_key[theme_key].parent_theme_key)
                if parent_key is None:
                    continue
                parent_theme_id = theme_id_by_key.get(parent_key)
                child_theme_id = theme_id_by_key.get(theme_key)
                if parent_theme_id is None or child_theme_id is None:
                    continue
                edge_key = (parent_theme_id, child_theme_id)
                if parent_theme_id == child_theme_id or edge_key in added_edges:
                    continue
                self._session.add(
                    ThemeHierarchyRelationship(
                        id=uuid.uuid4(),
                        codebook_id=codebook.id,
                        parent_theme_id=parent_theme_id,
                        child_theme_id=child_theme_id,
                        is_active=True,
                    )
                )
                added_edges.add(edge_key)

            codes_created = 0
            for code_draft in draft.codes:
                code_label = self._truncate_label(self._normalize_label(code_draft.label))
                code = Code(
                    id=uuid.uuid4(),
                    codebook_id=codebook.id,
                    label=code_label,
                    description=self._clean_optional_text(code_draft.description),
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
                theme_key = self._draft_key(code_draft.theme_key)
                if theme_key is not None and theme_key in theme_id_by_key:
                    self._session.add(
                        ThemeCodeRelationship(
                            id=uuid.uuid4(),
                            codebook_id=codebook.id,
                            theme_id=theme_id_by_key[theme_key],
                            code_id=code.id,
                            is_active=True,
                        )
                    )
                codes_created += 1

            validation = await ThemeGraphService(self._session).validate_theme_dag(
                codebook_id=codebook.id,
                ensure_codebook_exists=False,
            )
            if not validation.is_valid:
                violations = "; ".join(validation.violations)
                raise UnprocessableError(f"Generated hierarchy is invalid: {violations}")

            await self._session.commit()
            await self._session.refresh(codebook)
            return codebook, len(theme_id_by_key), codes_created
        except Exception:
            await self._session.rollback()
            raise

    async def _update_generated_token_totals(
        self,
        *,
        codebook_id: UUID,
        application_run_id: UUID,
        llm_tokens_input: int,
        llm_tokens_output: int,
    ) -> None:
        codebook = await self._session.get(Codebook, codebook_id)
        if codebook is not None:
            codebook.llm_tokens_input = llm_tokens_input
            codebook.llm_tokens_output = llm_tokens_output
        from app.models import CodebookApplicationRun

        application_run = await self._session.get(CodebookApplicationRun, application_run_id)
        if application_run is not None:
            application_run.llm_tokens_input = llm_tokens_input
            application_run.llm_tokens_output = llm_tokens_output
        await self._session.commit()

    def _enrich_provenance(
        self,
        *,
        generation_result: GenerationResult,
        loaded_algorithm: LoadedGenerationAlgorithm,
        random_seed: int,
        selected_document_ids: list[UUID],
        provider: str | None,
        algorithm_options: dict[str, object],
        application_run_id: UUID | None,
        documents_coded: int,
        documents_failed: int,
    ) -> dict[str, object]:
        provenance = self._json_dict(generation_result.provenance)
        provenance["generation_algorithm"] = {
            "module_spec": loaded_algorithm.spec,
            "algorithm_id": loaded_algorithm.algorithm.algorithm_id,
            "algorithm_version": loaded_algorithm.algorithm.algorithm_version,
            "source_sha256": loaded_algorithm.source_sha256,
            "random_seed": random_seed,
            "selected_document_ids": [str(document_id) for document_id in selected_document_ids],
            "llm_provider": provider,
            "llm_model": self._llm_model_for_provider(provider),
            "embedding_model": self._embedding_model_for_provider(provider),
            "token_usage": {
                "input_tokens": self._llm_tokens_input,
                "output_tokens": self._llm_tokens_output,
                "total_tokens": self._llm_tokens_input + self._llm_tokens_output,
                "algorithm_reported": self._json_dict(generation_result.token_usage),
            },
            "algorithm_options": self._json_dict(algorithm_options),
        }
        if application_run_id is not None:
            provenance["final_application"] = {
                "application_run_id": str(application_run_id),
                "documents_coded": documents_coded,
                "documents_failed": documents_failed,
            }
        return provenance

    @staticmethod
    def _with_action_ids(action_log: list[object]) -> list[dict[str, object]]:
        enriched: list[dict[str, object]] = []
        for index, action in enumerate(action_log, start=1):
            action_dict = action if isinstance(action, dict) else {"action": str(action)}
            action_with_id = {
                "action_id": f"act_{index:04d}",
                "inputs": action_dict.get("inputs", {}),
                "outputs": action_dict.get("outputs", {}),
                **action_dict,
            }
            enriched.append(cast(dict[str, object], action_with_id))
        return enriched

    @staticmethod
    def _token_count(result: GenerationResult, key: str) -> int:
        aliases = {
            "input_tokens": ("input_tokens", "llm_tokens_input", "prompt_tokens"),
            "output_tokens": ("output_tokens", "llm_tokens_output", "completion_tokens"),
        }
        for alias in aliases.get(key, (key,)):
            value = result.token_usage.get(alias)
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            if isinstance(value, str):
                try:
                    return int(value)
                except ValueError:
                    continue
        return 0

    @staticmethod
    def _json_dict(value: object) -> dict[str, object]:
        decoded = json.loads(json.dumps(value, ensure_ascii=False))
        if isinstance(decoded, dict):
            return cast(dict[str, object], decoded)
        return {}

    @staticmethod
    def _draft_key(value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = " ".join(value.split()).strip()
        return cleaned or None

    def _theme_depth(self, key: str, themes_by_key: dict[str, ThemeDraft]) -> int:
        current_key: str | None = key
        depth = 0
        seen: set[str] = set()
        while current_key is not None and current_key not in seen:
            seen.add(current_key)
            theme = themes_by_key.get(current_key)
            parent_key = self._draft_key(theme.parent_theme_key if theme is not None else None)
            current_key = parent_key if parent_key in themes_by_key else None
            depth += 1
        return depth

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
    def _llm_model_for_provider(provider: str | None) -> str | None:
        settings = get_settings()
        spec = providers.get_provider(provider)
        if spec is None:
            return settings.LLM_MODEL
        value = getattr(settings, spec.model_attr, None)
        return str(value) if value else None

    @staticmethod
    def _embedding_model_for_provider(provider: str | None) -> str | None:
        settings = get_settings()
        spec = providers.get_provider(provider)
        if spec is None:
            return settings.EMBEDDING_MODEL
        value = getattr(settings, spec.embedding_model_attr, None)
        return str(value) if value else None

    async def _persist_generated_codebook(
        self,
        *,
        codebook_name: str,
        corpus_id: UUID,
        research_query: str | None = None,
        researcher_topics: str | None = None,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
        code_nodes: list[_CodeDraft],
        hierarchy_edges: list[tuple[tuple[str, ...], tuple[str, ...]]],
        tracker: TokenTracker | None = None,
    ) -> tuple[Codebook, int, int]:
        try:
            version = await self._next_codebook_version(corpus_id=corpus_id)
            codebook = Codebook(
                id=uuid.uuid4(),
                corpus_id=corpus_id,
                name=codebook_name,
                description="LLM-generated codebook",
                version=version,
                created_by="system-llm",
                research_query=research_query,
                researcher_topics=researcher_topics,
                llm_tokens_input=tracker.input_tokens if tracker else None,
                llm_tokens_output=tracker.output_tokens if tracker else None,
            )
            self._session.add(codebook)
            await self._session.flush()

            ordered_theme_nodes = sorted(
                theme_nodes.values(), key=lambda node: (len(node.key), node.key)
            )
            theme_id_by_key: dict[tuple[str, ...], UUID] = {}
            theme_id_by_label: dict[str, UUID] = {}
            for node in ordered_theme_nodes:
                label_key = node.label.lower()
                existing_theme_id = theme_id_by_label.get(label_key)
                if existing_theme_id is not None:
                    # Multiple canonical keys can still share a label after LLM
                    # consolidation; persist only one Theme row per label.
                    theme_id_by_key[node.key] = existing_theme_id
                    continue

                theme = Theme(
                    id=uuid.uuid4(),
                    codebook_id=codebook.id,
                    label=node.label,
                    description=node.description,
                    is_active=True,
                )
                self._session.add(theme)
                await self._session.flush()
                theme_id_by_key[node.key] = theme.id
                theme_id_by_label[label_key] = theme.id
                self._session.add(
                    CodebookThemeRelationship(
                        id=uuid.uuid4(),
                        codebook_id=codebook.id,
                        theme_id=theme.id,
                        is_active=True,
                    )
                )

            parent_by_child: dict[UUID, UUID] = {}
            added_edges: set[tuple[UUID, UUID]] = set()
            for parent_key, child_key in hierarchy_edges:
                parent_theme_id = theme_id_by_key.get(parent_key)
                child_theme_id = theme_id_by_key.get(child_key)
                if parent_theme_id is None or child_theme_id is None:
                    continue
                if parent_theme_id == child_theme_id:
                    continue
                existing_parent = parent_by_child.get(child_theme_id)
                if existing_parent is not None and existing_parent != parent_theme_id:
                    # A label-merged child already has a parent in this codebook; keep first parent.
                    continue
                edge_key = (parent_theme_id, child_theme_id)
                if edge_key in added_edges:
                    continue
                self._session.add(
                    ThemeHierarchyRelationship(
                        id=uuid.uuid4(),
                        codebook_id=codebook.id,
                        parent_theme_id=parent_theme_id,
                        child_theme_id=child_theme_id,
                        is_active=True,
                    )
                )
                parent_by_child[child_theme_id] = parent_theme_id
                added_edges.add(edge_key)

            codes_created = 0
            for code_node in code_nodes:
                code = Code(
                    id=uuid.uuid4(),
                    codebook_id=codebook.id,
                    label=code_node.label,
                    description=code_node.description,
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
                if code_node.parent_theme_key:
                    parent_theme_id = theme_id_by_key.get(code_node.parent_theme_key)
                    if parent_theme_id:
                        self._session.add(
                            ThemeCodeRelationship(
                                id=uuid.uuid4(),
                                codebook_id=codebook.id,
                                theme_id=parent_theme_id,
                                code_id=code.id,
                                is_active=True,
                            )
                        )
                codes_created += 1

            # Validate before commit so an invalid generated hierarchy rolls
            # back atomically with its codebook, themes, and codes.
            validation = await ThemeGraphService(self._session).validate_theme_dag(
                codebook_id=codebook.id
            )
            if not validation.is_valid:
                violations = "; ".join(validation.violations)
                raise UnprocessableError(f"Generated hierarchy is invalid: {violations}")

            await self._session.commit()
            await self._session.refresh(codebook)
            return codebook, len(theme_id_by_label), codes_created
        except Exception:
            await self._session.rollback()
            raise

    async def _next_codebook_version(self, *, corpus_id: UUID) -> int:
        latest_version = (
            await self._session.execute(
                select(func.max(Codebook.version)).where(Codebook.corpus_id == corpus_id)
            )
        ).scalar_one_or_none()
        return int((latest_version or 0) + 1)
