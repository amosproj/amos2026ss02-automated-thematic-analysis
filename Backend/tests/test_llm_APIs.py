"""
cd Backend
pytest tests/test_llm_APIs.py -s

Live-call tests are skipped automatically unless the required API key is set.
  SELECTED_API=ACADEMIC → needs LLM_API_KEY
  SELECTED_API=FAU      → needs LLM_API_KEY_FAU
"""
import time
from pathlib import Path

import pytest

from app.config import get_settings
from app.llm import analyze_interview
from app.schemas import InterviewTranscript

INTERVIEW_FIXTURE = (
    Path(__file__).resolve().parent / "test-data" / "test_interview.jsonl"
)


def _active_api_key() -> str | None:
    """Return the API key for whichever provider is currently selected."""
    cfg = get_settings()
    if cfg.SELECTED_API.upper() == "FAU":
        return cfg.LLM_API_KEY_FAU
    return cfg.LLM_API_KEY


def _is_usable_key(value: str | None) -> bool:
    """Return True only for non-placeholder API key values."""
    if not value:
        return False
    stripped = value.strip()
    if not stripped:
        return False
    # Treat template placeholders like <your_key_here> as unset.
    if stripped.startswith("<") and stripped.endswith(">"):
        return False
    return True


requires_llm_key = pytest.mark.skipif(
    not _is_usable_key(_active_api_key()),
    reason=(
        f"No API key set for SELECTED_API='{get_settings().SELECTED_API}'. "
        "Set LLM_API_KEY_FAU (FAU) or LLM_API_KEY (ACADEMIC) to run live tests."
    ),
)

requires_academic_key = pytest.mark.skipif(
    not _is_usable_key(get_settings().LLM_API_KEY),
    reason="LLM_API_KEY not set; skipping live Academic Cloud call.",
)

requires_fau_key = pytest.mark.skipif(
    not _is_usable_key(get_settings().LLM_API_KEY_FAU),
    reason="LLM_API_KEY_FAU not set; skipping live NHR@FAU call.",
)


class _AssertionLog:
    def __init__(self) -> None:
        self.passed: list[str] = []

    def check(self, condition: bool, description: str) -> None:
        assert condition, description
        self.passed.append(description)

    def report(self, header: str) -> None:
        print(f"\n--- {header}: {len(self.passed)} assertion(s) passed ---")
        for i, msg in enumerate(self.passed, 1):
            print(f"  {i}. {msg}")


# ---------------------------------------------------------------------------
# Test 1: data handling — no LLM call, always runs
# ---------------------------------------------------------------------------

def test_offline_fixture_parses_into_dialog() -> None:
    log = _AssertionLog()
    transcript = InterviewTranscript.from_jsonl(INTERVIEW_FIXTURE)

    log.check(len(transcript.messages) > 0, "fixture contains at least one message")
    dialog = transcript.to_dialog_text()
    log.check("Interviewer:" in dialog, "rendered dialog includes Interviewer turns")
    log.check("Participant:" in dialog, "rendered dialog includes Participant turns")

    log.report("Fixture parsing")


# ---------------------------------------------------------------------------
# Shared helper for round-trip assertions (provider-agnostic)
# ---------------------------------------------------------------------------

def _assert_analyze_interview_response(log: _AssertionLog, settings, label: str) -> None:
    log.check(bool(settings.LLM_BASE_URL or settings.LLM_BASE_URL_FAU), "a base URL is configured")

    transcript = InterviewTranscript.from_jsonl(INTERVIEW_FIXTURE).to_dialog_text()
    started = time.perf_counter()
    answer = analyze_interview(transcript)
    elapsed = time.perf_counter() - started

    print(f"\n--- {label} LLM response ({elapsed:.1f}s) ---\n{answer}\n")

    log.check(isinstance(answer, str), "answer is a string")
    log.check(len(answer) > 200, "answer is expressive (>200 chars)")
    lowered = answer.lower()
    log.check(
        any(token in lowered for token in ("theme", "code", "summary")),
        "answer contains thematic-analysis structure (themes/codes/summary)",
    )
    log.check(
        elapsed < settings.LLM_REQUEST_TIMEOUT_S,
        f"call finished in {elapsed:.1f}s, under the {settings.LLM_REQUEST_TIMEOUT_S}s timeout",
    )


# ---------------------------------------------------------------------------
# Test 2: live round-trip against whichever provider is currently selected
# ---------------------------------------------------------------------------

@requires_llm_key
def test_live_api_call_selected_provider() -> None:
    """Live round-trip using whichever provider SELECTED_API currently points to."""
    log = _AssertionLog()
    get_settings.cache_clear()
    settings = get_settings()
    _assert_analyze_interview_response(log, settings, settings.SELECTED_API)
    log.report(f"{settings.SELECTED_API} round-trip")


# ---------------------------------------------------------------------------
# Test 3: explicit Academic Cloud live test (skipped if key missing)
# ---------------------------------------------------------------------------

@requires_academic_key
def test_live_api_call_academic_cloud() -> None:
    log = _AssertionLog()
    get_settings.cache_clear()

    import os
    original = os.environ.get("SELECTED_API")
    os.environ["SELECTED_API"] = "ACADEMIC"
    get_settings.cache_clear()

    try:
        _assert_analyze_interview_response(log, get_settings(), "Academic Cloud")
    finally:
        if original is None:
            os.environ.pop("SELECTED_API", None)
        else:
            os.environ["SELECTED_API"] = original
        get_settings.cache_clear()

    log.report("Academic Cloud round-trip")


# ---------------------------------------------------------------------------
# Test 4: explicit NHR@FAU live test (skipped if key missing)
# ---------------------------------------------------------------------------

@requires_fau_key
def test_live_api_call_nhr_fau_gateway() -> None:
    log = _AssertionLog()
    get_settings.cache_clear()

    import os
    original = os.environ.get("SELECTED_API")
    os.environ["SELECTED_API"] = "FAU"
    get_settings.cache_clear()

    try:
        _assert_analyze_interview_response(log, get_settings(), "NHR@FAU")
    finally:
        if original is None:
            os.environ.pop("SELECTED_API", None)
        else:
            os.environ["SELECTED_API"] = original
        get_settings.cache_clear()

    log.report("NHR@FAU round-trip")

