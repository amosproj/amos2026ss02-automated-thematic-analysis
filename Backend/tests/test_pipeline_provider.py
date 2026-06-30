"""The chain builders must forward the selected provider to build_chat_model.

This is the seam that carries the user's provider choice from a job runner all
the way to the LLM client, so it's worth pinning down directly.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.llm.pipelines import (
    build_codebook_application_with_codes_chain,
    build_codebook_generation_chain,
)


def test_application_chain_forwards_provider() -> None:
    with patch("app.llm.pipelines.build_chat_model") as mock_build:
        mock_build.return_value = MagicMock()
        build_codebook_application_with_codes_chain(provider="ACADEMIC")
        mock_build.assert_called_once_with(provider="ACADEMIC")


def test_generation_chain_forwards_provider() -> None:
    with patch("app.llm.pipelines.build_chat_model") as mock_build:
        mock_build.return_value = MagicMock()
        build_codebook_generation_chain(provider="FAU")
        mock_build.assert_called_once_with(provider="FAU")


def test_chain_uses_injected_model_over_provider() -> None:
    # When an explicit model is injected (tests, batching), build_chat_model is
    # not called at all — the provider is irrelevant in that path.
    sentinel_model = MagicMock()
    with patch("app.llm.pipelines.build_chat_model") as mock_build:
        build_codebook_application_with_codes_chain(model=sentinel_model, provider="ACADEMIC")
        mock_build.assert_not_called()
