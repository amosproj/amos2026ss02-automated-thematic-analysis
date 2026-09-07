from __future__ import annotations

from app.generation.contracts import (
    CodebookDraft,
    CodeDraft,
    GenerationAlgorithm,
    GenerationContext,
    GenerationInput,
    GenerationResult,
    ThemeDraft,
)


class DeterministicAlgorithm:
    requires_llm = False

    def __init__(self, *, suffix: str = "") -> None:
        self.algorithm_id = f"deterministic{suffix}"
        self.algorithm_version = "1.0"
        self._suffix = suffix

    async def generate(
        self,
        generation_input: GenerationInput,
        context: GenerationContext,
    ) -> GenerationResult:
        await context.set_phase("test_generating")
        await context.report_progress(
            len(generation_input.documents), len(generation_input.documents)
        )
        root_key = f"operations{self._suffix}"
        code_key = f"delay{self._suffix}"
        return GenerationResult(
            codebook=CodebookDraft(
                themes=(
                    ThemeDraft(
                        key=root_key,
                        label=f"Operations{self._suffix}",
                        description="Operational patterns.",
                    ),
                ),
                codes=(
                    CodeDraft(
                        key=code_key,
                        label=f"Delay{self._suffix}",
                        description="Delays in work.",
                        theme_key=root_key,
                    ),
                ),
            ),
            provenance={"test": True},
            action_log=({"action": "test_generate"},),
            processed_unit_count=len(generation_input.documents),
            quote_count=2,
            token_usage={"input_tokens": 3, "output_tokens": 4},
        )


def create() -> GenerationAlgorithm:
    return DeterministicAlgorithm()


def create_first() -> GenerationAlgorithm:
    return DeterministicAlgorithm(suffix=" First")


def create_second() -> GenerationAlgorithm:
    return DeterministicAlgorithm(suffix=" Second")
