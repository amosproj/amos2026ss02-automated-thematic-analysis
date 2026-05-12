"""Tests for app.schemas.llm – ThemePresence and InterviewAnalysisResult."""
from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from app.schemas.llm import InterviewAnalysisResult, ThemePresence


# ---------------------------------------------------------------------------
# ThemePresence
# ---------------------------------------------------------------------------

class TestThemePresence:
    def test_minimal_valid_present(self) -> None:
        tp = ThemePresence(
            theme_label="Cost Concerns",
            present=True,
            confidence=0.85,
            quote="it costs too much",
        )
        assert tp.theme_label == "Cost Concerns"
        assert tp.present is True
        assert tp.confidence == 0.85
        assert tp.quote == "it costs too much"

    def test_minimal_valid_absent(self) -> None:
        tp = ThemePresence(
            theme_label="Innovation",
            present=False,
            confidence=0.1,
        )
        assert tp.present is False
        assert tp.quote is None  # default

    def test_quote_defaults_to_none(self) -> None:
        tp = ThemePresence(theme_label="X", present=False, confidence=0.0)
        assert tp.quote is None

    def test_confidence_boundary_zero(self) -> None:
        tp = ThemePresence(theme_label="X", present=False, confidence=0.0)
        assert tp.confidence == 0.0

    def test_confidence_boundary_one(self) -> None:
        tp = ThemePresence(theme_label="X", present=True, confidence=1.0, quote="q")
        assert tp.confidence == 1.0

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            ThemePresence(present=True, confidence=0.5)  # type: ignore[call-arg]

    def test_serialization_round_trip(self) -> None:
        tp = ThemePresence(
            theme_label="Safety",
            present=True,
            confidence=0.9,
            quote="we felt safe",
        )
        data = json.loads(tp.model_dump_json())
        restored = ThemePresence(**data)
        assert restored == tp


# ---------------------------------------------------------------------------
# InterviewAnalysisResult
# ---------------------------------------------------------------------------

class TestInterviewAnalysisResult:
    def test_full_result(self) -> None:
        result = InterviewAnalysisResult(
            themes=[
                ThemePresence(theme_label="A", present=True, confidence=0.8, quote="q1"),
                ThemePresence(theme_label="B", present=False, confidence=0.1),
            ],
            summary="About the interview.",
            researcher_notes="Follow up on topic B.",
        )
        assert len(result.themes) == 2
        assert result.summary == "About the interview."
        assert result.researcher_notes == "Follow up on topic B."

    def test_optional_fields_default_to_none(self) -> None:
        result = InterviewAnalysisResult(
            themes=[ThemePresence(theme_label="X", present=False, confidence=0.0)],
        )
        assert result.summary is None
        assert result.researcher_notes is None

    def test_empty_themes_list_is_valid(self) -> None:
        result = InterviewAnalysisResult(themes=[])
        assert result.themes == []

    def test_missing_themes_raises(self) -> None:
        with pytest.raises(ValidationError):
            InterviewAnalysisResult()  # type: ignore[call-arg]

    def test_from_dict_matches_llm_json_shape(self) -> None:
        """Simulate the exact JSON shape the LLM returns."""
        raw = {
            "summary": "Short summary.",
            "researcher_notes": None,
            "themes": [
                {
                    "theme_label": "Collaboration",
                    "present": True,
                    "confidence": 0.92,
                    "quote": "we work together",
                },
                {
                    "theme_label": "Cost",
                    "present": False,
                    "confidence": 0.05,
                    "quote": None,
                },
            ],
        }
        result = InterviewAnalysisResult(**raw)
        assert len(result.themes) == 2
        assert result.themes[0].present is True
        assert result.themes[1].quote is None

    def test_serialization_round_trip(self) -> None:
        result = InterviewAnalysisResult(
            themes=[
                ThemePresence(theme_label="T1", present=True, confidence=0.7, quote="q"),
            ],
            summary="s",
            researcher_notes="n",
        )
        data = json.loads(result.model_dump_json())
        restored = InterviewAnalysisResult(**data)
        assert restored == result
