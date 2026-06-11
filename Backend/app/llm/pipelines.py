import json
from typing import Any

from langchain_core.language_models import BaseChatModel
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.runnables import Runnable, RunnableConfig

from app.llm.client import build_chat_model
from app.llm.prompts import (
    _build_research_query_block,
    _build_researcher_topics_block,
    build_code_consolidation_prompt,
    build_codebook_application_prompt,
    build_codebook_generation_prompt,
    build_thematic_analysis_prompt,
    build_theme_consolidation_prompt,
)
from app.schemas.llm import (
    CodeConsolidationItem,
    CodeConsolidationResult,
    GeneratedThemePath,
    InterviewAnalysisResult,
    PassageCodebookGeneration,
    ThemeConsolidationResult,
)


# Run a single-shot thematic analysis over a transcript.
def analyze_interview(
    transcript: str,
    *,
    model: BaseChatModel | None = None,
) -> str:

    if not transcript.strip():
        raise ValueError("Transcript is empty.")

    chain = build_thematic_analysis_prompt() | (model or build_chat_model()) | StrOutputParser()
    return chain.invoke({"transcript": transcript})


# Apply a predefined codebook to an interview transcript.
def apply_codebook_to_interview(
    transcript: str,
    codebook_context: str,
    *,
    model: BaseChatModel | None = None,
) -> InterviewAnalysisResult:

    if not transcript.strip():
        raise ValueError("Transcript is empty.")
    if not codebook_context.strip():
        raise ValueError("Codebook context is empty.")

    chat_model = model or build_chat_model()
    # Use JsonOutputParser instead of with_structured_output for broader compatibility
    parser = JsonOutputParser(pydantic_object=InterviewAnalysisResult)
    chain = build_codebook_application_prompt() | chat_model | parser
    raw_result = chain.invoke({"transcript": transcript, "codebook": codebook_context})
    return InterviewAnalysisResult(**raw_result)


def build_codebook_generation_chain(
    *,
    model: BaseChatModel | None = None,
) -> Runnable[dict[str, str], dict[str, Any]]:
    chat_model = model or build_chat_model()
    parser = JsonOutputParser(pydantic_object=PassageCodebookGeneration)
    return build_codebook_generation_prompt() | chat_model | parser


# Generate candidate themes/subthemes/codes for a single transcript passage.
def generate_codebook_for_passage(
    passage: str,
    *,
    research_query: str | None = None,
    researcher_topics: str | None = None,
    model: BaseChatModel | None = None,
) -> PassageCodebookGeneration:
    if not passage.strip():
        raise ValueError("Passage is empty.")

    chain = build_codebook_generation_chain(model=model)
    raw_result = chain.invoke({
        "passage": passage,
        "research_query_block": _build_research_query_block(research_query or ""),
        "researcher_topics_block": _build_researcher_topics_block(researcher_topics or ""),
    })
    return PassageCodebookGeneration(**raw_result)


async def generate_codebook_for_passages(
    passages: list[str],
    *,
    chain: Runnable[dict[str, str], dict[str, Any]] | None = None,
    model: BaseChatModel | None = None,
    max_concurrency: int | None = None,
    research_query: str | None = None,
    researcher_topics: str | None = None,
) -> list[PassageCodebookGeneration | Exception]:
    if not passages:
        return []
    for passage in passages:
        if not passage.strip():
            raise ValueError("Passage is empty.")

    runnable = chain or build_codebook_generation_chain(model=model)
    # Researcher focus is constant across the batch, so build the blocks once.
    research_query_block = _build_research_query_block(research_query or "")
    researcher_topics_block = _build_researcher_topics_block(researcher_topics or "")
    config: RunnableConfig | None = (
        {"max_concurrency": max_concurrency} if max_concurrency is not None else None
    )
    raw_results = await runnable.abatch(
        [
            {
                "passage": passage,
                "research_query_block": research_query_block,
                "researcher_topics_block": researcher_topics_block,
            }
            for passage in passages
        ],
        config=config,
        return_exceptions=True,
    )

    parsed_results: list[PassageCodebookGeneration | Exception] = []
    for raw_result in raw_results:
        if isinstance(raw_result, Exception):
            parsed_results.append(raw_result)
            continue
        try:
            parsed_results.append(PassageCodebookGeneration(**raw_result))
        except Exception as exc:
            parsed_results.append(exc)
    return parsed_results


def consolidate_generated_codes(
    codes: list[CodeConsolidationItem],
    *,
    model: BaseChatModel | None = None,
) -> CodeConsolidationResult:
    """Merge overlapping generated codes into a smaller orthogonal set."""
    if not codes:
        return CodeConsolidationResult(codes=[])

    chat_model = model or build_chat_model()
    parser = JsonOutputParser(pydantic_object=CodeConsolidationResult)
    # Serialize as formatted JSON so the model receives a stable, explicit list.
    serialized_codes = json.dumps(
        [code.model_dump(mode="json") for code in codes],
        ensure_ascii=True,
        indent=2,
    )
    chain = build_code_consolidation_prompt() | chat_model | parser
    raw_result = chain.invoke({"codes": serialized_codes})
    return CodeConsolidationResult(**raw_result)


def consolidate_generated_themes(
    themes: list[GeneratedThemePath],
    *,
    constraints: str | None = None,
    model: BaseChatModel | None = None,
) -> ThemeConsolidationResult:
    """Merge overlapping generated theme paths into a smaller coherent hierarchy."""
    if not themes:
        return ThemeConsolidationResult(themes=[])

    chat_model = model or build_chat_model()
    parser = JsonOutputParser(pydantic_object=ThemeConsolidationResult)
    # Serialize as formatted JSON so the model receives a stable, explicit list.
    serialized_themes = json.dumps(
        [theme.model_dump(mode="json") for theme in themes],
        ensure_ascii=True,
        indent=2,
    )
    chain = build_theme_consolidation_prompt() | chat_model | parser
    raw_result = chain.invoke(
        {
            "themes": serialized_themes,
            "constraints": constraints
            or "- Use domain-level roots only.\n- Target 6-10 root themes.\n- Keep total themes compact and non-overlapping.",
        }
    )
    return ThemeConsolidationResult(**raw_result)

