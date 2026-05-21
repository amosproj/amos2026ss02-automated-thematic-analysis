"""
Unit tests for app.llm.client.build_chat_model().

These tests are fully offline (no real LLM calls) — they verify that
build_chat_model() picks the correct credentials based on SELECTED_API,
and raises clearly when a key is missing.

Adding a new provider in the future? Add:
  1. A Settings override with the new SELECTED_API value
  2. A test_build_<provider>_selects_correct_credentials() test
  3. A test_build_<provider>_raises_when_key_missing() test
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from app.config import Settings
from app.llm.client import build_chat_model

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _settings(**overrides) -> Settings:
    """Build a minimal Settings instance without a real .env / DATABASE_URL."""
    base = dict(DATABASE_URL="sqlite+aiosqlite:///:memory:")
    base.update(overrides)
    return Settings(**base)


# ---------------------------------------------------------------------------
# SELECTED_API=FAU
# ---------------------------------------------------------------------------

class TestFAUProvider:
    def test_build_fau_selects_correct_credentials(self) -> None:
        """With SELECTED_API=FAU, build_chat_model must use FAU base URL and key."""
        cfg = _settings(
            SELECTED_API="FAU",
            LLM_API_KEY_FAU="fau-test-key",
            LLM_BASE_URL_FAU="https://hub.nhr.fau.de/api/llmgw/v1",
            LLM_MODEL_FAU="gpt-oss-120b",
        )
        with patch("app.llm.client.ChatOpenAI") as mock_cls:
            build_chat_model(settings=cfg)
            call_kwargs = mock_cls.call_args.kwargs
            assert call_kwargs["api_key"] == "fau-test-key"
            assert call_kwargs["base_url"] == "https://hub.nhr.fau.de/api/llmgw/v1"
            assert call_kwargs["model"] == "gpt-oss-120b"

    def test_build_fau_raises_when_key_missing(self) -> None:
        """With SELECTED_API=FAU but no LLM_API_KEY_FAU, must raise RuntimeError."""
        cfg = _settings(SELECTED_API="FAU", LLM_API_KEY_FAU=None)
        with pytest.raises(RuntimeError, match="LLM_API_KEY_FAU"):
            build_chat_model(settings=cfg)

    def test_build_fau_case_insensitive(self) -> None:
        """SELECTED_API matching should be case-insensitive ('fau', 'Fau', 'FAU')."""
        for value in ("fau", "Fau", "FAU"):
            cfg = _settings(
                SELECTED_API=value,
                LLM_API_KEY_FAU="fau-key",
                LLM_BASE_URL_FAU="https://hub.nhr.fau.de/api/llmgw/v1",
                LLM_MODEL_FAU="gpt-oss-120b",
            )
            with patch("app.llm.client.ChatOpenAI") as mock_cls:
                build_chat_model(settings=cfg)
                assert mock_cls.call_args.kwargs["api_key"] == "fau-key"


# ---------------------------------------------------------------------------
# SELECTED_API=ACADEMIC
# ---------------------------------------------------------------------------

class TestAcademicProvider:
    def test_build_academic_selects_correct_credentials(self) -> None:
        """With SELECTED_API=ACADEMIC, build_chat_model must use Academic Cloud credentials."""
        cfg = _settings(
            SELECTED_API="ACADEMIC",
            LLM_API_KEY="academic-test-key",
            LLM_BASE_URL="https://chat-ai.academiccloud.de/v1",
            LLM_MODEL="gemma-3-27b-it",
        )
        with patch("app.llm.client.ChatOpenAI") as mock_cls:
            build_chat_model(settings=cfg)
            call_kwargs = mock_cls.call_args.kwargs
            assert call_kwargs["api_key"] == "academic-test-key"
            assert call_kwargs["base_url"] == "https://chat-ai.academiccloud.de/v1"
            assert call_kwargs["model"] == "gemma-3-27b-it"

    def test_build_academic_raises_when_key_missing(self) -> None:
        """With SELECTED_API=ACADEMIC but no LLM_API_KEY, must raise RuntimeError."""
        cfg = _settings(SELECTED_API="ACADEMIC", LLM_API_KEY=None)
        with pytest.raises(RuntimeError, match="LLM_API_KEY"):
            build_chat_model(settings=cfg)

    def test_build_academic_case_insensitive(self) -> None:
        """SELECTED_API='academic' and 'Academic' should both route to Academic Cloud."""
        for value in ("academic", "Academic", "ACADEMIC"):
            cfg = _settings(
                SELECTED_API=value,
                LLM_API_KEY="academic-key",
                LLM_BASE_URL="https://chat-ai.academiccloud.de/v1",
                LLM_MODEL="gemma-3-27b-it",
            )
            with patch("app.llm.client.ChatOpenAI") as mock_cls:
                build_chat_model(settings=cfg)
                assert mock_cls.call_args.kwargs["api_key"] == "academic-key"


# ---------------------------------------------------------------------------
# Model / temperature overrides (provider-agnostic)
# ---------------------------------------------------------------------------

class TestOverrides:
    def test_model_override_is_passed_through(self) -> None:
        """An explicit model= argument must override the default from settings."""
        cfg = _settings(
            SELECTED_API="FAU",
            LLM_API_KEY_FAU="key",
            LLM_MODEL_FAU="gpt-oss-120b",
        )
        with patch("app.llm.client.ChatOpenAI") as mock_cls:
            build_chat_model(settings=cfg, model="my-custom-model")
            assert mock_cls.call_args.kwargs["model"] == "my-custom-model"

    def test_temperature_override_is_passed_through(self) -> None:
        """An explicit temperature= argument must override LLM_TEMPERATURE from settings."""
        cfg = _settings(
            SELECTED_API="ACADEMIC",
            LLM_API_KEY="key",
            LLM_TEMPERATURE=0.2,
        )
        with patch("app.llm.client.ChatOpenAI") as mock_cls:
            build_chat_model(settings=cfg, temperature=0.9)
            assert mock_cls.call_args.kwargs["temperature"] == 0.9

    def test_default_temperature_comes_from_settings(self) -> None:
        cfg = _settings(
            SELECTED_API="ACADEMIC",
            LLM_API_KEY="key",
            LLM_TEMPERATURE=0.42,
        )
        with patch("app.llm.client.ChatOpenAI") as mock_cls:
            build_chat_model(settings=cfg)
            assert mock_cls.call_args.kwargs["temperature"] == 0.42


# ---------------------------------------------------------------------------
# Unknown / future providers fall back to ACADEMIC
# ---------------------------------------------------------------------------

class TestUnknownProvider:
    def test_unknown_selected_api_falls_back_to_academic(self) -> None:
        """Any unrecognised SELECTED_API value falls back to Academic Cloud credentials."""
        cfg = _settings(
            SELECTED_API="LITELLM",           # hypothetical future value
            LLM_API_KEY="academic-fallback",
            LLM_BASE_URL="https://chat-ai.academiccloud.de/v1",
            LLM_MODEL="gemma-3-27b-it",
        )
        with patch("app.llm.client.ChatOpenAI") as mock_cls:
            build_chat_model(settings=cfg)
            assert mock_cls.call_args.kwargs["api_key"] == "academic-fallback"
