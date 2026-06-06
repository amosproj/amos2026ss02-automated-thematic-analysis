"""Unit tests for the sanitize_research_query utility."""
from __future__ import annotations

from app.utils.sanitize import sanitize_research_query


class TestSanitizeResearchQuery:
    def test_plain_text_passes_through(self) -> None:
        value = "How do researchers describe barriers to adoption?"
        assert sanitize_research_query(value) == value

    def test_html_tags_are_stripped(self) -> None:
        result = sanitize_research_query("<b>bold</b> question")
        assert "<b>" not in result
        assert "bold question" in result

    def test_script_tag_is_removed(self) -> None:
        result = sanitize_research_query('<script>alert("xss")</script>How are themes formed?')
        assert "<script>" not in result
        assert "alert" not in result
        assert "How are themes formed?" in result

    def test_script_tag_with_space_before_closing_angle_is_removed(self) -> None:
        result = sanitize_research_query('<script >alert("xss")</script >How are themes formed?')
        assert "alert" not in result
        assert "How are themes formed?" in result

    def test_sql_injection_like_string_is_preserved_literally(self) -> None:
        value = "What about users who say ' OR 1=1 -- in interviews?"
        result = sanitize_research_query(value)
        assert "OR 1=1" in result

    def test_control_characters_are_removed(self) -> None:
        value = "Hello\x00\x07World"
        result = sanitize_research_query(value)
        assert "\x00" not in result
        assert "\x07" not in result
        assert "HelloWorld" in result

    def test_newlines_are_preserved(self) -> None:
        value = "Line one\nLine two"
        result = sanitize_research_query(value)
        assert "\n" in result

    def test_leading_and_trailing_whitespace_stripped(self) -> None:
        result = sanitize_research_query("  a question here  ")
        assert result == "a question here"

    def test_internal_whitespace_collapsed(self) -> None:
        result = sanitize_research_query("too   many    spaces")
        assert result == "too many spaces"

    def test_exactly_10_chars_after_sanitise(self) -> None:
        value = "1234567890"
        assert len(sanitize_research_query(value)) == 10

    def test_9_chars_after_strip(self) -> None:
        value = "  123456789  "
        result = sanitize_research_query(value)
        assert len(result) == 9

    def test_500_chars_accepted(self) -> None:
        value = "a" * 500
        assert len(sanitize_research_query(value)) == 500

    def test_html_leaving_less_than_10_chars_shortens_output(self) -> None:
        value = "<b></b><i></i>hi"
        result = sanitize_research_query(value)
        assert result == "hi"

    def test_empty_string_returns_empty(self) -> None:
        assert sanitize_research_query("") == ""

    def test_only_html_tags_returns_empty(self) -> None:
        assert sanitize_research_query("<div><span></span></div>") == ""

    def test_nested_html_stripped(self) -> None:
        result = sanitize_research_query("<div><p>content</p></div>")
        assert result == "content"
