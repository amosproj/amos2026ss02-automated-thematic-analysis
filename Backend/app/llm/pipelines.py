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
    build_codebook_application_with_codes_prompt,
    build_codebook_generation_prompt,
    build_thematic_analysis_prompt,
    build_theme_consolidation_prompt,
)
from app.schemas.llm import (
    CodebookApplicationResult,
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

    # LangChain pipes each stage into the next: prompt -> model -> parser.
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
    # JsonOutputParser keeps the chain compatible with providers that do not
    # implement LangChain's with_structured_output API.
    parser = JsonOutputParser(pydantic_object=InterviewAnalysisResult)
    # The prompt receives the input dict, the chat model returns text, and the
    # parser converts that text into a plain dict matching the Pydantic schema.
    chain = build_codebook_application_prompt() | chat_model | parser
    raw_result = chain.invoke({"transcript": transcript, "codebook": codebook_context})
    return InterviewAnalysisResult(**raw_result)


def build_codebook_application_with_codes_chain(
    *,
    model: BaseChatModel | None = None,
) -> Runnable[dict[str, str], dict[str, Any]]:
    chat_model = model or build_chat_model()
    # The parser validates the model output shape but returns a dict, so callers
    # still instantiate CodebookApplicationResult explicitly after invocation.
    parser = JsonOutputParser(pydantic_object=CodebookApplicationResult)
    return build_codebook_application_with_codes_prompt() | chat_model | parser


async def apply_codebook_with_codes_to_transcript(
    transcript: str,
    codebook_context: str,
    *,
    chain: Runnable[dict[str, str], dict[str, Any]] | None = None,
    model: BaseChatModel | None = None,
) -> CodebookApplicationResult:
    if not transcript.strip():
        raise ValueError("Transcript is empty.")
    if not codebook_context.strip():
        raise ValueError("Codebook context is empty.")

    # Accepting an injected chain keeps tests deterministic and avoids rebuilding
    # the model client for each document.
    runnable = chain or build_codebook_application_with_codes_chain(model=model)
    raw_result = await runnable.ainvoke({"transcript": transcript, "codebook": codebook_context})
    return CodebookApplicationResult(**raw_result)


async def apply_codebook_with_codes_to_transcripts(
    transcripts: list[str],
    codebook_context: str,
    *,
    chain: Runnable[dict[str, str], dict[str, Any]] | None = None,
    model: BaseChatModel | None = None,
    max_concurrency: int | None = None,
) -> list[CodebookApplicationResult | Exception]:
    if not transcripts:
        return []
    for transcript in transcripts:
        if not transcript.strip():
            raise ValueError("Transcript is empty.")
    if not codebook_context.strip():
        raise ValueError("Codebook context is empty.")

    # Reuse one Runnable across the batch; LangChain handles parallel execution
    # inside abatch according to the optional max_concurrency config below.
    runnable = chain or build_codebook_application_with_codes_chain(model=model)
    config: RunnableConfig | None = (
        {"max_concurrency": max_concurrency} if max_concurrency is not None else None
    )
    # return_exceptions=True preserves the input order and lets the service retry
    # only the documents that failed, instead of failing the whole batch.
    raw_results = await runnable.abatch(
        [
            {
                "transcript": transcript,
                "codebook": codebook_context,
            }
            for transcript in transcripts
        ],
        config=config,
        return_exceptions=True,
    )

    # Normalize LangChain/provider/parser outcomes into one positional result
    # list so the service can map each item back to its original document.
    parsed_results: list[CodebookApplicationResult | Exception] = []
    for raw_result in raw_results:
        if isinstance(raw_result, Exception):
            parsed_results.append(raw_result)
            continue
        try:
            parsed_results.append(CodebookApplicationResult(**raw_result))
        except Exception as exc:
            parsed_results.append(exc)
    return parsed_results


def build_codebook_generation_chain(
    *,
    model: BaseChatModel | None = None,
) -> Runnable[dict[str, str], dict[str, Any]]:
    chat_model = model or build_chat_model()
    # See build_codebook_application_with_codes_chain: this Runnable returns
    # parsed dictionaries that are converted to Pydantic models by callers.
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

    # The generated prompt expects both researcher focus blocks, even when they
    # are empty, so the template can keep a stable set of variables.
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

    # The same LangChain Runnable is reused for every passage in the batch.
    runnable = chain or build_codebook_generation_chain(model=model)
    # Researcher focus is constant across the batch, so build the blocks once.
    research_query_block = _build_research_query_block(research_query or "")
    researcher_topics_block = _build_researcher_topics_block(researcher_topics or "")
    config: RunnableConfig | None = (
        {"max_concurrency": max_concurrency} if max_concurrency is not None else None
    )
    # abatch returns results in input order. Keeping exceptions as values lets
    # the generation service retry or record individual passage failures.
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

    # Convert successful dicts to schema objects while leaving failures in the
    # same positions for the caller's retry bookkeeping.
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
    # Consolidation is a single prompt/model/parser chain because it merges an
    # already prepared JSON payload, not a list of independent inputs.
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
    # The constraints string is injected as one prompt variable so callers can
    # tune how aggressively LangChain asks the model to merge theme paths.
    chain = build_theme_consolidation_prompt() | chat_model | parser
    raw_result = chain.invoke(
        {
            "themes": serialized_themes,
            "constraints": constraints
            or "- Use domain-level roots only.\n- Target 6-10 root themes.\n- Keep total themes compact and non-overlapping.",
        }
    )
    return ThemeConsolidationResult(**raw_result)

