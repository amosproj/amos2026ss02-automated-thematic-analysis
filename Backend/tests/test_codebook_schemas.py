"""Tests for CodebookGenerateRequest validation, especially research_query."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.schemas.codebook import CodebookGenerateRequest

_VALID_CORPUS_ID = "00000000-0000-0000-0000-000000000001"
_VALID_QUERY = "How do participants describe barriers to technology adoption at work?"


def _make_request(**overrides) -> dict:
    base = {
        "codebook_name": "My Codebook",
        "corpus_id": _VALID_CORPUS_ID,
        "research_query": _VALID_QUERY,
    }
    base.update(overrides)
    return base


class TestResearchQueryValidation:
    def test_valid_query_accepted(self) -> None:
        req = CodebookGenerateRequest(**_make_request())
        assert req.research_query == _VALID_QUERY

    def test_missing_query_accepted(self) -> None:
        # research_query is optional; omitting it should yield None.
        data = _make_request()
        del data["research_query"]
        req = CodebookGenerateRequest(**data)
        assert req.research_query is None

    def test_empty_query_treated_as_none(self) -> None:
        # An empty string is normalised to None by the sanitiser.
        req = CodebookGenerateRequest(**_make_request(research_query=""))
        assert req.research_query is None

    def test_9_char_query_raises(self) -> None:
        # Optional, but once provided it must meet the 10-char minimum.
        with pytest.raises(ValidationError):
            CodebookGenerateRequest(**_make_request(research_query="123456789"))

    def test_10_char_query_accepted(self) -> None:
        req = CodebookGenerateRequest(**_make_request(research_query="1234567890"))
        assert len(req.research_query) == 10

    def test_500_char_query_accepted(self) -> None:
        query = "a" * 500
        req = CodebookGenerateRequest(**_make_request(research_query=query))
        assert len(req.research_query) == 500

    def test_501_char_query_raises(self) -> None:
        with pytest.raises(ValidationError):
            CodebookGenerateRequest(**_make_request(research_query="a" * 501))

    def test_whitespace_only_query_raises(self) -> None:
        # A non-empty value that is only whitespace is rejected (not silently
        # treated as "no query").
        with pytest.raises(ValidationError):
            CodebookGenerateRequest(**_make_request(research_query="          "))

    def test_html_tags_stripped_before_length_check(self) -> None:
        # "<b>" is 3 chars of tag; remaining text is 5 chars → below min after strip.
        with pytest.raises(ValidationError):
            CodebookGenerateRequest(**_make_request(research_query="<b>short</b>"))

    def test_html_stripped_but_enough_content_accepted(self) -> None:
        query = "<b>" + "a" * 20 + "</b>"
        req = CodebookGenerateRequest(**_make_request(research_query=query))
        assert "<b>" not in req.research_query
        assert len(req.research_query) >= 10

    def test_script_tag_stripped(self) -> None:
        query = '<script>alert(1)</script>' + "a" * 20
        req = CodebookGenerateRequest(**_make_request(research_query=query))
        assert "script" not in req.research_query
        assert "alert" not in req.research_query

    def test_sql_injection_like_string_accepted(self) -> None:
        query = "What do users mean when they say ' OR 1=1 in conversations?"
        req = CodebookGenerateRequest(**_make_request(research_query=query))
        assert "OR 1=1" in req.research_query
