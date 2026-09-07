from __future__ import annotations

from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession

from app.generation.contracts import (
    GenerationAlgorithm,
    GenerationCancelledError,
    GenerationContext,
    GenerationInput,
    GenerationResult,
)
from app.services.traceable_analysis import (
    TraceableAnalysisCancelledError,
    TraceableAnalysisService,
)


class TraceableGenerationAlgorithm:
    algorithm_id = "traceable_analysis"
    algorithm_version = "1.0"
    requires_llm = True

    async def generate(
        self,
        generation_input: GenerationInput,
        context: GenerationContext,
    ) -> GenerationResult:
        service = TraceableAnalysisService(cast(AsyncSession, None))
        max_refinement_rounds = _int_option(
            generation_input.algorithm_options.get("max_refinement_rounds"),
            default=5,
        )
        try:
            return await service.generate_draft(
                documents=list(generation_input.documents),
                research_query=generation_input.research_query,
                researcher_topics=generation_input.researcher_topics,
                max_refinement_rounds=max_refinement_rounds,
                provider=context.selected_llm_provider,
                on_unit_progress=context.on_progress,
                on_phase_progress=context.on_phase_progress,
                on_phase=context.on_phase,
                should_cancel=context.should_cancel,
            )
        except TraceableAnalysisCancelledError as exc:
            raise GenerationCancelledError("Codebook generation was cancelled") from exc


def create() -> GenerationAlgorithm:
    return TraceableGenerationAlgorithm()


def _int_option(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default
