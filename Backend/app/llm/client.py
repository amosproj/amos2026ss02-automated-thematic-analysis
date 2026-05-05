from langchain_openai import ChatOpenAI

from app.config import Settings, get_settings


def build_chat_model(
    settings: Settings | None = None,
    *,
    model: str | None = None,
    temperature: float | None = None,
) -> ChatOpenAI:
    cfg = settings or get_settings()
    if not cfg.LLM_API_KEY:
        raise RuntimeError(
            "LLM_API_KEY is not set. Provide an Academic Cloud Chat AI key "
            "(or a LiteLLM gateway key) via the environment."
        )
    return ChatOpenAI(
        model=model or cfg.LLM_MODEL,
        temperature=cfg.LLM_TEMPERATURE if temperature is None else temperature,
        base_url=cfg.LLM_BASE_URL,
        api_key=cfg.LLM_API_KEY,
        timeout=cfg.LLM_REQUEST_TIMEOUT_S,
    )


# TODO two options, invoke vs invoke_streaming: 
# For a single-shot analysis, invoke should sufficient 
# Streaming is more relevant for corpus analysis or to provide real-time feedback to the user.
