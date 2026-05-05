from langchain_core.language_models import BaseChatModel
from langchain_core.output_parsers import StrOutputParser

from app.llm.client import build_chat_model
from app.llm.prompts import build_thematic_analysis_prompt

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
