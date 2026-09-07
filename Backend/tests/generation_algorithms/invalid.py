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


class MissingGenerate:
    algorithm_id = "missing_generate"
    algorithm_version = "1.0"


class SyncGenerate:
    algorithm_id = "sync_generate"
    algorithm_version = "1.0"

    def generate(
        self, _generation_input: GenerationInput, _context: GenerationContext
    ) -> GenerationResult:
        raise AssertionError("not async")


class EmptyId:
    algorithm_id = ""
    algorithm_version = "1.0"

    async def generate(
        self,
        generation_input: GenerationInput,
        context: GenerationContext,
    ) -> GenerationResult:
        del generation_input, context
        raise AssertionError("not used")


class MissingRequiresLlm:
    algorithm_id = "missing_requires_llm"
    algorithm_version = "1.0"

    async def generate(
        self,
        generation_input: GenerationInput,
        context: GenerationContext,
    ) -> GenerationResult:
        del generation_input, context
        raise AssertionError("not used")


class InvalidDraftAlgorithm:
    requires_llm = False
    algorithm_id = "invalid_draft"
    algorithm_version = "1.0"

    async def generate(
        self,
        generation_input: GenerationInput,
        context: GenerationContext,
    ) -> GenerationResult:
        del generation_input, context
        return GenerationResult(
            codebook=CodebookDraft(
                themes=(
                    ThemeDraft(key="theme_a", label="Duplicate"),
                    ThemeDraft(key="theme_b", label=" duplicate "),
                ),
                codes=(CodeDraft(key="code_a", label="Code", theme_key="theme_a"),),
            )
        )


def create_missing_generate() -> object:
    return MissingGenerate()


def create_sync_generate() -> object:
    return SyncGenerate()


def create_empty_id() -> object:
    return EmptyId()


def create_missing_requires_llm() -> object:
    return MissingRequiresLlm()


def create_raises() -> object:
    raise RuntimeError("factory exploded")


def create_invalid_draft() -> GenerationAlgorithm:
    return InvalidDraftAlgorithm()
