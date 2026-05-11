"""Tests for the codebook-application prompts and the build_codebook_application_prompt helper."""
from __future__ import annotations

from langchain_core.prompts import ChatPromptTemplate

from app.llm.prompts import (
    APPLY_CODEBOOK_SYSTEM_PROMPT,
    APPLY_CODEBOOK_USER_INSTRUCTION,
    build_codebook_application_prompt,
)


class TestCodebookPromptContent:
    """Verify the prompt strings contain the key instructions."""

    def test_system_prompt_mentions_deductive(self) -> None:
        assert "deductive" in APPLY_CODEBOOK_SYSTEM_PROMPT.lower()

    def test_system_prompt_mentions_confidence(self) -> None:
        assert "confidence" in APPLY_CODEBOOK_SYSTEM_PROMPT.lower()

    def test_system_prompt_mentions_json(self) -> None:
        assert "json" in APPLY_CODEBOOK_SYSTEM_PROMPT.lower()

    def test_system_prompt_mentions_verbatim_quote(self) -> None:
        assert "verbatim" in APPLY_CODEBOOK_SYSTEM_PROMPT.lower()

    def test_user_instruction_has_codebook_placeholder(self) -> None:
        assert "{codebook}" in APPLY_CODEBOOK_USER_INSTRUCTION

    def test_user_instruction_has_transcript_placeholder(self) -> None:
        assert "{transcript}" in APPLY_CODEBOOK_USER_INSTRUCTION


class TestBuildCodebookApplicationPrompt:
    def test_returns_chat_prompt_template(self) -> None:
        prompt = build_codebook_application_prompt()
        assert isinstance(prompt, ChatPromptTemplate)

    def test_prompt_has_two_messages(self) -> None:
        prompt = build_codebook_application_prompt()
        assert len(prompt.messages) == 2

    def test_prompt_accepts_expected_variables(self) -> None:
        prompt = build_codebook_application_prompt()
        input_vars = prompt.input_variables
        assert "codebook" in input_vars
        assert "transcript" in input_vars

    def test_prompt_can_format_without_error(self) -> None:
        prompt = build_codebook_application_prompt()
        messages = prompt.format_messages(
            codebook="Theme: Trust\nDefinition: ...",
            transcript="I trust my team.",
        )
        assert len(messages) == 2
        # System message comes first
        assert "deductive" in messages[0].content.lower()
        # User message contains both placeholders expanded
        assert "I trust my team." in messages[1].content
        assert "Theme: Trust" in messages[1].content
