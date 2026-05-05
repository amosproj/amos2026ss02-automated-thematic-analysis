"""
cd Backend
pytest tests/test_llm_academic_cloud.py -s

The live-call test is skipped automatically unless ``LLM_API_KEY`` is set.
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

requires_llm_key = pytest.mark.skipif(
    not get_settings().LLM_API_KEY,
    reason="LLM_API_KEY not set (checked OS env and .env); skipping live Academic Cloud call.",
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


# Test 1: data handling and prompt formatting (no LLM call, just fixture parsing and dialog rendering)
def test_jsonl_fixture_parses_into_dialog() -> None:
    log = _AssertionLog()
    transcript = InterviewTranscript.from_jsonl(INTERVIEW_FIXTURE)

    log.check(len(transcript.messages) > 0, "fixture contains at least one message")
    dialog = transcript.to_dialog_text()
    log.check("Interviewer:" in dialog, "rendered dialog includes Interviewer turns")
    log.check("Participant:" in dialog, "rendered dialog includes Participant turns")

    log.report("Fixture parsing")


# Test 2: full round-trip against Academic Cloud
@requires_llm_key
def test_analyze_interview_against_academic_cloud() -> None:
    log = _AssertionLog()
    get_settings.cache_clear()  # pick up freshly exported env vars
    settings = get_settings()

    log.check(bool(settings.LLM_BASE_URL), "LLM_BASE_URL is configured")
    log.check(bool(settings.LLM_MODEL), "LLM_MODEL is configured")

    transcript = InterviewTranscript.from_jsonl(INTERVIEW_FIXTURE).to_dialog_text()

    started = time.perf_counter()
    answer = analyze_interview(transcript)
    elapsed = time.perf_counter() - started

    print(f"\n--- LLM response ({elapsed:.1f}s) ---\n{answer}\n")

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

    log.report("Academic Cloud round-trip")
