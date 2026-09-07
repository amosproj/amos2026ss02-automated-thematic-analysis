from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Protocol, TypeAlias
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

JsonValue: TypeAlias = object

ProgressCallback: TypeAlias = Callable[[int, int], Awaitable[None]]
PhaseCallback: TypeAlias = Callable[[str], Awaitable[None]]
PhaseProgressCallback: TypeAlias = Callable[[str, int, int], Awaitable[None]]
CancellationCallback: TypeAlias = Callable[[], Awaitable[bool]]


class GenerationCancelledError(Exception):
    """Raised by algorithms when the core cancellation callback is set."""


class _FrozenGenerationModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
        arbitrary_types_allowed=True,
    )


class GenerationDocument(_FrozenGenerationModel):
    document_id: UUID
    title: str
    content: str


class GenerationInput(_FrozenGenerationModel):
    documents: tuple[GenerationDocument, ...]
    research_query: str | None = None
    researcher_topics: str | None = None
    random_seed: int = Field(ge=0)
    algorithm_options: Mapping[str, JsonValue] = Field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GenerationContext:
    selected_llm_provider: str | None = None
    llm_model: str | None = None
    embedding_model: str | None = None
    on_progress: ProgressCallback | None = None
    on_phase: PhaseCallback | None = None
    on_phase_progress: PhaseProgressCallback | None = None
    should_cancel: CancellationCallback | None = None

    async def set_phase(self, phase: str) -> None:
        if self.on_phase is not None:
            await self.on_phase(phase)

    async def report_progress(self, done: int, total: int) -> None:
        if self.on_progress is not None:
            await self.on_progress(done, total)

    async def report_phase_progress(self, phase: str, done: int, total: int) -> None:
        if self.on_phase_progress is not None:
            await self.on_phase_progress(phase, done, total)

    async def raise_if_cancelled(self) -> None:
        if self.should_cancel is not None and await self.should_cancel():
            raise GenerationCancelledError("Codebook generation was cancelled")


class ThemeDraft(_FrozenGenerationModel):
    key: str
    label: str
    description: str | None = None
    parent_theme_key: str | None = None


class CodeDraft(_FrozenGenerationModel):
    label: str
    key: str | None = None
    description: str | None = None
    theme_key: str | None = None


class CodebookDraft(_FrozenGenerationModel):
    themes: tuple[ThemeDraft, ...] = Field(default_factory=tuple)
    codes: tuple[CodeDraft, ...] = Field(default_factory=tuple)


class GenerationResult(_FrozenGenerationModel):
    codebook: CodebookDraft
    provenance: Mapping[str, JsonValue] = Field(default_factory=dict)
    action_log: tuple[Mapping[str, JsonValue], ...] = Field(default_factory=tuple)
    processed_unit_count: int = Field(default=0, ge=0)
    quote_count: int = Field(default=0, ge=0)
    token_usage: Mapping[str, JsonValue] = Field(default_factory=dict)


class GenerationAlgorithm(Protocol):
    algorithm_id: str
    algorithm_version: str
    requires_llm: bool

    async def generate(
        self,
        generation_input: GenerationInput,
        context: GenerationContext,
    ) -> GenerationResult: ...
