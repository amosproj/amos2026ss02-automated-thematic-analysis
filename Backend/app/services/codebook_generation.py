from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from langchain_core.exceptions import OutputParserException
from loguru import logger
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError, UnprocessableError
from app.llm.pipelines import (
    consolidate_generated_codes,
    consolidate_generated_themes,
    generate_codebook_for_passage,
)
from app.models import (
    Code,
    Codebook,
    CodebookCodeRelationship,
    CodebookThemeRelationship,
    Corpus,
    CorpusChunk,
    CorpusDocument,
    Theme,
    ThemeHierarchyRelationship,
)
from app.schemas.codebook import CodebookSchema, GeneratedCodebookResponse
from app.schemas.llm import (
    CodeConsolidationItem,
    GeneratedThemeNode,
    GeneratedThemePath,
    PassageCodebookGeneration,
)
from app.services.theme_graph import ThemeGraphService


@dataclass
class _ThemeNodeDraft:
    key: tuple[str, ...]
    label: str
    description: str | None = None


@dataclass
class _CodeDraft:
    label: str
    description: str | None


class CodebookGenerationCancelledError(Exception):
    pass


class CodebookGenerationService:
    """Generate and persist a new codebook from selected transcript chunks."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def generate_codebook(
        self,
        *,
        codebook_name: str,
        corpus_id: UUID,
        transcript_document_ids: list[UUID] | None,
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
    ) -> GeneratedCodebookResponse:
        normalized_document_ids = self._deduplicate_document_ids(transcript_document_ids)

        corpus = await self._load_corpus(corpus_id)
        project_id = str(corpus.project_id)
        documents = await self._load_documents(
            corpus_id=corpus_id,
            transcript_document_ids=normalized_document_ids,
        )
        if not documents:
            raise UnprocessableError("No documents found in the selected corpus")
        passages = await self._load_passages(
            corpus_id=corpus_id,
            transcript_document_ids=[document.id for document in documents],
        )
        if not passages:
            raise UnprocessableError("No transcript passages found for selected transcript_document_ids")

        # End the read transaction before long-running LLM calls so the session
        # does not keep a checked-out DB connection during inference.
        await self._session.rollback()

        generation_results, failed_passages = await self._generate_per_passage(
            passages,
            on_progress=on_progress,
            should_cancel=should_cancel,
        )
        await self._raise_if_cancelled(should_cancel)
        theme_nodes, code_nodes, hierarchy_edges = self._deduplicate_generation(generation_results)
        code_nodes = await self._post_process_codes(code_nodes, should_cancel=should_cancel)
        await self._raise_if_cancelled(should_cancel)
        theme_nodes, hierarchy_edges = await self._post_process_themes(
            theme_nodes=theme_nodes,
            hierarchy_edges=hierarchy_edges,
            should_cancel=should_cancel,
        )
        await self._raise_if_cancelled(should_cancel)
        if not theme_nodes:
            raise UnprocessableError(
                "Codebook generation produced no themes from selected passages "
                f"(failed passages: {len(failed_passages)})"
            )

        created_codebook, themes_created, codes_created = await self._persist_generated_codebook(
            codebook_name=codebook_name,
            project_id=project_id,
            theme_nodes=theme_nodes,
            code_nodes=code_nodes,
            hierarchy_edges=hierarchy_edges,
        )
        return GeneratedCodebookResponse(
            codebook=CodebookSchema.model_validate(created_codebook),
            transcripts_processed=len(documents),
            passages_processed=len(passages),
            themes_created=themes_created,
            codes_created=codes_created,
            passages_failed=len(failed_passages),
            failed_passages=failed_passages,
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
        missing = [document_id for document_id in transcript_document_ids if document_id not in documents_by_id]
        if missing:
            missing_str = ", ".join(str(document_id) for document_id in missing)
            raise UnprocessableError(
                "Some transcript_document_ids were not found in the selected corpus: "
                f"{missing_str}"
            )
        return [documents_by_id[document_id] for document_id in transcript_document_ids]

    async def _load_passages(
        self,
        *,
        corpus_id: UUID,
        transcript_document_ids: list[UUID],
    ) -> list[str]:
        chunk_rows = list(
            (
                await self._session.execute(
                    select(CorpusChunk.document_id, CorpusChunk.text, CorpusChunk.chunk_index)
                    .join(CorpusDocument, CorpusChunk.document_id == CorpusDocument.id)
                    .where(
                        CorpusDocument.corpus_id == corpus_id,
                        CorpusChunk.document_id.in_(transcript_document_ids),
                    )
                    .order_by(CorpusChunk.document_id, CorpusChunk.chunk_index)
                )
            ).all()
        )
        if not chunk_rows:
            return []

        chunks_by_document: dict[UUID, list[tuple[int, str]]] = {document_id: [] for document_id in transcript_document_ids}
        for document_id, text, chunk_index in chunk_rows:
            chunks_by_document[document_id].append((chunk_index, text))

        passages: list[str] = []
        for document_id in transcript_document_ids:
            ordered_chunks = sorted(chunks_by_document.get(document_id, []), key=lambda row: row[0])
            passages.extend(text.strip() for _, text in ordered_chunks if text and text.strip())
        return passages

    @staticmethod
    async def _generate_per_passage(
        passages: list[str],
        *,
        on_progress: Callable[[int, int], Awaitable[None]] | None = None,
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
    ) -> tuple[list[PassageCodebookGeneration], list[GeneratedCodebookResponse.PassageFailure]]:
        results: list[PassageCodebookGeneration] = []
        failures: list[GeneratedCodebookResponse.PassageFailure] = []
        total = len(passages)
        if on_progress is not None:
            await on_progress(0, total)

        for index, passage in enumerate(passages, start=1):
            parse_error: OutputParserException | ValidationError | None = None
            attempts = 3
            for attempt in range(1, attempts + 1):
                if should_cancel is not None and await should_cancel():
                    raise CodebookGenerationCancelledError("Codebook generation was cancelled")
                try:
                    generation = await asyncio.to_thread(generate_codebook_for_passage, passage)
                    results.append(generation)
                    parse_error = None
                    break
                except OutputParserException as exc:
                    parse_error = exc
                    if attempt < attempts:
                        continue
                except ValidationError as exc:
                    parse_error = exc
                    if attempt < attempts:
                        continue
                except CodebookGenerationCancelledError:
                    raise
                except Exception as exc:
                    raise UnprocessableError(f"Codebook generation failed: {exc}") from exc

            if parse_error is not None:
                failures.append(
                    GeneratedCodebookResponse.PassageFailure(
                        passage_index=index,
                        passage_excerpt=passage[:240],
                        error=str(parse_error),
                        attempts=attempts,
                    )
                )
            if on_progress is not None:
                await on_progress(index, total)
        return results, failures

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
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
    ) -> list[_CodeDraft]:
        """Consolidate generated codes and keep a deterministic fallback."""
        await self._raise_if_cancelled(should_cancel)
        if not codes:
            return []
        if len(codes) == 1:
            # No overlap resolution needed for a single code.
            return codes

        consolidation_payload = [
            CodeConsolidationItem(label=code.label, description=code.description)
            for code in codes
        ]
        original_labels = [code.label for code in codes]
        try:
            consolidated = await asyncio.to_thread(
                consolidate_generated_codes,
                consolidation_payload,
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
            consolidated_codes.append(
                _CodeDraft(
                    label=normalized_label,
                    description=description or None,
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
        removed_labels = sorted([label for label in original_labels if label.lower() not in kept_label_keys])
        logger.info(
            "Code consolidation finished: before={before}, after={after}, removed={removed}",
            before=len(original_labels),
            after=len(consolidated_labels),
            removed=len(removed_labels),
        )
        logger.debug("Code consolidation kept labels: {}", consolidated_labels)
        logger.debug("Code consolidation removed labels: {}", removed_labels)
        return consolidated_codes

    async def _post_process_themes(
        self,
        *,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
        hierarchy_edges: list[tuple[tuple[str, ...], tuple[str, ...]]],
        should_cancel: Callable[[], Awaitable[bool]] | None = None,
    ) -> tuple[dict[tuple[str, ...], _ThemeNodeDraft], list[tuple[tuple[str, ...], tuple[str, ...]]]]:
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

        consolidated_theme_nodes, consolidated_edges = self._build_theme_graph_from_paths(consolidated.themes)
        if not consolidated_theme_nodes:
            logger.warning(
                "Theme consolidation returned no usable themes; using pre-consolidation tree (themes={count})",
                count=len(theme_nodes),
            )
            return theme_nodes, hierarchy_edges

        # If first pass remains too broad, run a stricter compression pass.
        consolidated_root_count = self._count_root_themes(consolidated_edges, consolidated_theme_nodes)
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
                )
                await self._raise_if_cancelled(should_cancel)
                strict_nodes, strict_edges = self._build_theme_graph_from_paths(strict_consolidated.themes)
                if strict_nodes:
                    consolidated = strict_consolidated
                    consolidated_theme_nodes = strict_nodes
                    consolidated_edges = strict_edges
            except CodebookGenerationCancelledError:
                raise
            except Exception:
                logger.exception("Strict theme consolidation pass failed; using first-pass consolidated tree")

        original_labels = sorted({node.label for node in theme_nodes.values()})
        consolidated_labels = sorted({node.label for node in consolidated_theme_nodes.values()})
        kept_label_keys = {label.lower() for label in consolidated_labels}
        removed_labels = sorted([label for label in original_labels if label.lower() not in kept_label_keys])

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
            "- Be highly aggressive: collapse near-duplicates and subordinate variants unless analytically necessary.\n"
            "- Do not keep narrow examples (specific jobs, incidents, or anecdotes) as Level-1 or Level-2 themes.\n"
        ) if aggressive else ""
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
    ) -> tuple[dict[tuple[str, ...], _ThemeNodeDraft], list[tuple[tuple[str, ...], tuple[str, ...]]]]:
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
                    raw_edges.append((tuple(part.lower() for part in normalized_labels[: index - 1]), key))

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
        for parent, child in sorted(raw_edges, key=lambda pair: (len(pair[0]), pair[0], len(pair[1]), pair[1])):
            canonical_parent = canonical_key_by_original.get(parent)
            canonical_child = canonical_key_by_original.get(child)
            if canonical_parent is None or canonical_child is None:
                continue
            if canonical_parent == canonical_child:
                continue
            existing_parent = child_parent.get(canonical_child)
            if existing_parent is not None and existing_parent != canonical_parent:
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
    ) -> tuple[dict[tuple[str, ...], _ThemeNodeDraft], list[_CodeDraft], list[tuple[tuple[str, ...], tuple[str, ...]]]]:
        theme_nodes_by_key: dict[tuple[str, ...], _ThemeNodeDraft] = {}
        codes_by_key: dict[str, _CodeDraft] = {}
        raw_edges: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

        for result in generation_results:
            for generated_path in result.themes:
                normalized_labels = [cls._normalize_label(node.label) for node in generated_path.path]
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
                        raw_edges.append((tuple(part.lower() for part in normalized_labels[: index - 1]), key))

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
                        description=generated_code.description.strip() if generated_code.description else None,
                    )
                elif not existing_code.description and generated_code.description:
                    description = generated_code.description.strip()
                    if description:
                        existing_code.description = description

        sorted_codes = sorted(
            codes_by_key.values(),
            key=lambda code: code.label.lower(),
        )
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
        for parent, child in sorted(raw_edges, key=lambda pair: (len(pair[0]), pair[0], len(pair[1]), pair[1])):
            canonical_parent = canonical_key_by_original.get(parent)
            canonical_child = canonical_key_by_original.get(child)
            if canonical_parent is None or canonical_child is None:
                continue
            if canonical_parent == canonical_child:
                continue
            existing_parent = child_parent.get(canonical_child)
            if existing_parent is not None and existing_parent != canonical_parent:
                continue
            edge = (canonical_parent, canonical_child)
            if edge in seen_edges:
                continue
            seen_edges.add(edge)
            child_parent[canonical_child] = canonical_parent
            canonical_edges.append(edge)

        return canonical_theme_nodes, sorted_codes, canonical_edges

    async def _persist_generated_codebook(
        self,
        *,
        codebook_name: str,
        project_id: str,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
        code_nodes: list[_CodeDraft],
        hierarchy_edges: list[tuple[tuple[str, ...], tuple[str, ...]]],
    ) -> tuple[Codebook, int, int]:
        try:
            version = await self._next_codebook_version(project_id=project_id)
            codebook = Codebook(
                id=uuid.uuid4(),
                project_id=project_id,
                name=codebook_name,
                description="LLM-generated codebook",
                version=version,
                created_by="system-llm",
            )
            self._session.add(codebook)
            await self._session.flush()

            ordered_theme_nodes = sorted(theme_nodes.values(), key=lambda node: (len(node.key), node.key))
            theme_id_by_key: dict[tuple[str, ...], UUID] = {}
            theme_id_by_label: dict[str, UUID] = {}
            for node in ordered_theme_nodes:
                label_key = node.label.lower()
                existing_theme_id = theme_id_by_label.get(label_key)
                if existing_theme_id is not None:
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
                codes_created += 1

            validation = await ThemeGraphService(self._session).validate_theme_dag(codebook_id=codebook.id)
            if not validation.is_valid:
                violations = "; ".join(validation.violations)
                raise UnprocessableError(f"Generated hierarchy is invalid: {violations}")

            await self._session.commit()
            await self._session.refresh(codebook)
            return codebook, len(theme_id_by_label), codes_created
        except Exception:
            await self._session.rollback()
            raise

    async def _next_codebook_version(self, *, project_id: str) -> int:
        latest_version = (
            await self._session.execute(
                select(func.max(Codebook.version)).where(Codebook.project_id == project_id)
            )
        ).scalar_one_or_none()
        return int((latest_version or 0) + 1)
