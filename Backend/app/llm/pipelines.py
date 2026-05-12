from langchain_core.language_models import BaseChatModel
from langchain_core.output_parsers import StrOutputParser

from app.llm.client import build_chat_model
from app.llm.prompts import build_thematic_analysis_prompt, build_codebook_application_prompt
from app.schemas.llm import InterviewAnalysisResult

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

from langchain_core.output_parsers import JsonOutputParser

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

