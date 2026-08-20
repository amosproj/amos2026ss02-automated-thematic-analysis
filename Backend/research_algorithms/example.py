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


class ExampleGenerationAlgorithm:
    """Minimal local algorithm template.

    Copy this file, rename the class if you want, and edit generate(). Keep the
    create() factory at the bottom so GENERATION_ALGORITHM can point to it.
    """

    algorithm_id = "research_example"
    algorithm_version = "0.1"
    requires_llm = False

    async def generate(
        self,
        generation_input: GenerationInput,
        context: GenerationContext,
    ) -> GenerationResult:
        await context.set_phase("research_algorithm_generating")
        await context.report_progress(0, len(generation_input.documents))
        await context.raise_if_cancelled()

        # Researchers usually edit only this block.
        # You receive immutable DTOs, not ORM rows:
        # - generation_input.documents: document_id, title, content
        # - generation_input.research_query and researcher_topics
        # - generation_input.random_seed for reproducible randomized choices
        # - generation_input.algorithm_options for core-provided options
        #
        # If you need an LLM, set requires_llm = True and use the selected
        # provider with the existing client:
        #
        #   from app.llm.client import build_chat_model
        #   model = build_chat_model(provider=context.selected_llm_provider)
        #   response = await model.ainvoke("Your prompt")
        #
        # Do not import app.models, open a SQLAlchemy session, or write rows.
        document_count = len(generation_input.documents)
        for done in range(1, document_count + 1):
            await context.raise_if_cancelled()
            await context.report_progress(done, document_count)

        root_theme_key = "research_focus"
        code_key = "notable_pattern"
        return GenerationResult(
            codebook=CodebookDraft(
                themes=(
                    ThemeDraft(
                        key=root_theme_key,
                        label="Research Focus",
                        description="A starter theme produced by the local example algorithm.",
                    ),
                ),
                codes=(
                    CodeDraft(
                        key=code_key,
                        label="Notable Pattern",
                        description="Replace this with a code derived from the selected transcripts.",
                        theme_key=root_theme_key,
                    ),
                ),
            ),
            provenance={
                "method": "example deterministic local algorithm",
                "documents_seen": document_count,
                "random_seed": generation_input.random_seed,
            },
            action_log=(
                {
                    "action": "generate_example_codebook",
                    "documents": document_count,
                },
            ),
            processed_unit_count=document_count,
            quote_count=0,
            token_usage={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        )


def create() -> GenerationAlgorithm:
    return ExampleGenerationAlgorithm()
