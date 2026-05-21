from langchain_openai import ChatOpenAI

from app.config import Settings, get_settings


def build_chat_model(
    settings: Settings | None = None,
    *,
    model: str | None = None,
    temperature: float | None = None,
) -> ChatOpenAI:
    cfg = settings or get_settings()

    # Resolve credentials based on SELECTED_API
    if cfg.SELECTED_API.upper() == "FAU":
        api_key  = cfg.LLM_API_KEY_FAU
        base_url = cfg.LLM_BASE_URL_FAU
        default_model = cfg.LLM_MODEL_FAU
    else:  # "ACADEMIC" or any other value falls back to the Academic Cloud config
        api_key  = cfg.LLM_API_KEY
        base_url = cfg.LLM_BASE_URL
        default_model = cfg.LLM_MODEL

    if not api_key:
        raise RuntimeError(
            f"No API key set for SELECTED_API='{cfg.SELECTED_API}'. "
            "Set LLM_API_KEY_FAU (for FAU) or LLM_API_KEY (for ACADEMIC) in your .env file."
        )
    return ChatOpenAI(
        model=model or default_model,
        temperature=cfg.LLM_TEMPERATURE if temperature is None else temperature,
        base_url=base_url,
        api_key=api_key,
        timeout=cfg.LLM_REQUEST_TIMEOUT_S,
    )


# TODO two options, invoke vs invoke_streaming:
# For a single-shot analysis, invoke should sufficient
# Streaming is more relevant for corpus analysis or to provide real-time feedback to the user.
