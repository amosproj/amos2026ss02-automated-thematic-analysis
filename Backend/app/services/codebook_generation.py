from __future__ import annotations

import uuid
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import NotFoundError, UnprocessableError
from app.llm.pipelines import generate_codebook_for_passage
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
from app.schemas.llm import PassageCodebookGeneration
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


class CodebookGenerationService:
    """Generate and persist a new codebook from selected transcript chunks."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def generate_codebook(
        self,
        *,
        codebook_name: str,
        corpus_id: UUID,
        transcript_document_ids: list[UUID],
    ) -> GeneratedCodebookResponse:
        normalized_document_ids = self._deduplicate_document_ids(transcript_document_ids)
        if not normalized_document_ids:
            raise UnprocessableError("transcript_document_ids must contain at least one document id")

        corpus = await self._load_corpus(corpus_id)
        documents = await self._load_documents(
            corpus_id=corpus_id,
            transcript_document_ids=normalized_document_ids,
        )
        passages = await self._load_passages(
            corpus_id=corpus_id,
            transcript_document_ids=[document.id for document in documents],
        )
        if not passages:
            raise UnprocessableError("No transcript passages found for selected transcript_document_ids")

        generation_results = self._generate_per_passage(passages)
        theme_nodes, code_nodes = self._deduplicate_generation(generation_results)
        if not theme_nodes:
            raise UnprocessableError("Codebook generation produced no themes from selected passages")

        created_codebook, themes_created, codes_created = await self._persist_generated_codebook(
            codebook_name=codebook_name,
            project_id=str(corpus.project_id),
            theme_nodes=theme_nodes,
            code_nodes=code_nodes,
        )
        return GeneratedCodebookResponse(
            codebook=CodebookSchema.model_validate(created_codebook),
            transcripts_processed=len(documents),
            passages_processed=len(passages),
            themes_created=themes_created,
            codes_created=codes_created,
        )

    @staticmethod
    def _deduplicate_document_ids(document_ids: list[UUID]) -> list[UUID]:
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
    def _generate_per_passage(passages: list[str]) -> list[PassageCodebookGeneration]:
        results: list[PassageCodebookGeneration] = []
        for passage in passages:
            try:
                results.append(generate_codebook_for_passage(passage))
            except Exception as exc:
                raise UnprocessableError(f"Codebook generation failed: {exc}") from exc
        return results

    @staticmethod
    def _normalize_label(value: str) -> str:
        return " ".join(value.split()).strip()

    @classmethod
    def _deduplicate_generation(
        cls,
        generation_results: list[PassageCodebookGeneration],
    ) -> tuple[dict[tuple[str, ...], _ThemeNodeDraft], list[_CodeDraft]]:
        theme_nodes_by_key: dict[tuple[str, ...], _ThemeNodeDraft] = {}
        codes_by_key: dict[str, _CodeDraft] = {}

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
        return theme_nodes_by_key, sorted_codes

    async def _persist_generated_codebook(
        self,
        *,
        codebook_name: str,
        project_id: str,
        theme_nodes: dict[tuple[str, ...], _ThemeNodeDraft],
        code_nodes: list[_CodeDraft],
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
            for node in ordered_theme_nodes:
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
                self._session.add(
                    CodebookThemeRelationship(
                        id=uuid.uuid4(),
                        codebook_id=codebook.id,
                        theme_id=theme.id,
                        is_active=True,
                    )
                )

            for node in ordered_theme_nodes:
                if len(node.key) <= 1:
                    continue
                parent_key = node.key[:-1]
                parent_theme_id = theme_id_by_key.get(parent_key)
                child_theme_id = theme_id_by_key.get(node.key)
                if parent_theme_id is None or child_theme_id is None:
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
            return codebook, len(theme_id_by_key), codes_created
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
