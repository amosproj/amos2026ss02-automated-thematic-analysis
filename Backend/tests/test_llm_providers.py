"""Unit tests for the LLM provider registry (app.llm.providers)."""
from __future__ import annotations

from app.config import Settings
from app.llm import providers


def _settings(**overrides) -> Settings:
    base = dict(DATABASE_URL="sqlite+aiosqlite:///:memory:")
    base.update(overrides)
    return Settings(**base)


def test_available_providers_contains_both_modes() -> None:
    ids = [spec.id for spec in providers.available_providers()]
    assert ids == ["FAU", "ACADEMIC"]


def test_default_provider_is_fau() -> None:
    assert providers.DEFAULT_PROVIDER_ID == "FAU"


def test_normalize_is_case_insensitive() -> None:
    assert providers.normalize("fau") == "FAU"
    assert providers.normalize("Academic") == "ACADEMIC"


def test_normalize_unknown_returns_none() -> None:
    assert providers.normalize("litellm") is None
    assert providers.normalize("") is None
    assert providers.normalize(None) is None


def test_is_known_provider() -> None:
    assert providers.is_known_provider("FAU")
    assert not providers.is_known_provider("nope")
    assert not providers.is_known_provider(None)


def test_resolve_default_uses_selected_api() -> None:
    assert providers.resolve_default(_settings(SELECTED_API="ACADEMIC")) == "ACADEMIC"


def test_resolve_default_falls_back_to_fau_for_unknown() -> None:
    assert providers.resolve_default(_settings(SELECTED_API="LITELLM")) == "FAU"


def test_has_api_key_reflects_settings() -> None:
    cfg = _settings(LLM_API_KEY_FAU="key", LLM_API_KEY=None)
    assert providers.has_api_key(cfg, "FAU") is True
    assert providers.has_api_key(cfg, "ACADEMIC") is False
    assert providers.has_api_key(cfg, "UNKNOWN") is False
