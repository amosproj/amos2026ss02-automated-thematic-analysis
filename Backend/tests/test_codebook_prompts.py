"""Tests for the codebook-application prompts and the build_codebook_application_prompt helper."""
from __future__ import annotations

from langchain_core.prompts import ChatPromptTemplate

from app.llm.prompts import (
    APPLY_CODEBOOK_SYSTEM_PROMPT,
    APPLY_CODEBOOK_USER_INSTRUCTION,
    CODE_CONSOLIDATION_SYSTEM_PROMPT,
    CODE_CONSOLIDATION_USER_INSTRUCTION,
    THEME_CONSOLIDATION_SYSTEM_PROMPT,
    THEME_CONSOLIDATION_USER_INSTRUCTION,
    build_code_consolidation_prompt,
    build_codebook_application_prompt,
    build_theme_consolidation_prompt,
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


class TestCodeConsolidationPrompt:
    def test_system_prompt_mentions_merge_and_orthogonal(self) -> None:
        prompt_text = CODE_CONSOLIDATION_SYSTEM_PROMPT.lower()
        assert "merge" in prompt_text
        assert "orthogonal" in prompt_text
        assert "theme_path" in CODE_CONSOLIDATION_SYSTEM_PROMPT

    def test_user_instruction_has_codes_placeholder(self) -> None:
        assert "{codes}" in CODE_CONSOLIDATION_USER_INSTRUCTION

    def test_build_prompt_accepts_codes_variable(self) -> None:
        prompt = build_code_consolidation_prompt()
        assert isinstance(prompt, ChatPromptTemplate)
        assert "codes" in prompt.input_variables


class TestThemeConsolidationPrompt:
    def test_system_prompt_mentions_merge_and_hierarchy(self) -> None:
        prompt_text = THEME_CONSOLIDATION_SYSTEM_PROMPT.lower()
        assert "merge" in prompt_text
        assert "hierarchy" in prompt_text

    def test_user_instruction_has_themes_placeholder(self) -> None:
        assert "{themes}" in THEME_CONSOLIDATION_USER_INSTRUCTION
        assert "{constraints}" in THEME_CONSOLIDATION_USER_INSTRUCTION

    def test_build_prompt_accepts_themes_variable(self) -> None:
        prompt = build_theme_consolidation_prompt()
        assert isinstance(prompt, ChatPromptTemplate)
        assert "themes" in prompt.input_variables
        assert "constraints" in prompt.input_variables

    def test_theme_prompt_can_format_with_required_variables(self) -> None:
        prompt = build_theme_consolidation_prompt()
        messages = prompt.format_messages(
            themes='[{"path":[{"label":"Workflow Friction"}]}]',
            constraints="- Keep Level-1 roots at <= 10.",
        )
        assert len(messages) == 2
        assert "Workflow Friction" in messages[1].content
        assert "Level-1 roots" in messages[1].content
