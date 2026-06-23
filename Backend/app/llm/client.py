from langchain_openai import ChatOpenAI

from app.config import Settings, get_settings
from app.llm import providers


def build_chat_model(
    settings: Settings | None = None,
    *,
    provider: str | None = None,
    model: str | None = None,
    temperature: float | None = None,
) -> ChatOpenAI:
    cfg = settings or get_settings()

    # Resolve which provider to use: an explicit argument (e.g. the active
    # provider read from the DB at job-run start) wins; otherwise fall back to
    # the env-configured SELECTED_API default so existing callers and scripts
    # keep working with zero config.
    provider_id = providers.normalize(provider) or providers.resolve_default(cfg)
    spec = providers.get_provider(provider_id)
    # resolve_default always returns a known id, so spec is never None here.
    assert spec is not None  # noqa: S101 - registry invariant

    api_key = getattr(cfg, spec.api_key_attr)
    base_url = getattr(cfg, spec.base_url_attr)
    default_model = getattr(cfg, spec.model_attr)

    if not api_key:
        raise RuntimeError(
            f"No API key set for LLM provider '{provider_id}' ({spec.label}). "
            f"Set {spec.api_key_attr} in your .env file."
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
