from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher


@dataclass(frozen=True)
class QuoteMatch:
    quote: str
    start_char: int | None
    end_char: int | None
    quote_match_status: str


def locate_quote_span(transcript: str, quote: str | None) -> QuoteMatch:
    """Locate an LLM-returned quote in a transcript with conservative fallbacks."""

    cleaned_quote = (quote or "").strip()
    if not transcript or not cleaned_quote:
        return QuoteMatch(quote=cleaned_quote, start_char=None, end_char=None, quote_match_status="not_found")

    exact_start = transcript.find(cleaned_quote)
    if exact_start >= 0:
        return QuoteMatch(
            quote=cleaned_quote,
            start_char=exact_start,
            end_char=exact_start + len(cleaned_quote),
            quote_match_status="exact",
        )

    normalized_text, text_mapping = _normalize_with_mapping(transcript)
    normalized_quote, _ = _normalize_with_mapping(cleaned_quote)
    if normalized_quote:
        normalized_start = normalized_text.find(normalized_quote)
        if normalized_start >= 0:
            return _match_from_normalized_position(
                quote=cleaned_quote,
                normalized_start=normalized_start,
                normalized_length=len(normalized_quote),
                mapping=text_mapping,
                status="normalized",
            )

    fuzzy_match = _locate_fuzzy_match(normalized_text, normalized_quote, text_mapping)
    if fuzzy_match is not None:
        start_char, end_char = fuzzy_match
        return QuoteMatch(
            quote=cleaned_quote,
            start_char=start_char,
            end_char=end_char,
            quote_match_status="fuzzy",
        )

    return QuoteMatch(quote=cleaned_quote, start_char=None, end_char=None, quote_match_status="not_found")


def _normalize_with_mapping(value: str) -> tuple[str, list[int]]:
    normalized: list[str] = []
    mapping: list[int] = []
    previous_was_space = True
    for index, char in enumerate(value):
        if char.isspace():
            if normalized and not previous_was_space:
                normalized.append(" ")
                mapping.append(index)
            previous_was_space = True
            continue
        normalized.append(char)
        mapping.append(index)
        previous_was_space = False

    if normalized and normalized[-1] == " ":
        normalized.pop()
        mapping.pop()
    return "".join(normalized), mapping


def _match_from_normalized_position(
    *,
    quote: str,
    normalized_start: int,
    normalized_length: int,
    mapping: list[int],
    status: str,
) -> QuoteMatch:
    normalized_end = normalized_start + normalized_length - 1
    if not mapping or normalized_start < 0 or normalized_end >= len(mapping):
        return QuoteMatch(quote=quote, start_char=None, end_char=None, quote_match_status="not_found")
    return QuoteMatch(
        quote=quote,
        start_char=mapping[normalized_start],
        end_char=mapping[normalized_end] + 1,
        quote_match_status=status,
    )


def _locate_fuzzy_match(
    normalized_text: str,
    normalized_quote: str,
    mapping: list[int],
    *,
    threshold: float = 0.88,
) -> tuple[int, int] | None:
    if len(normalized_quote) < 12 or not normalized_text or not mapping:
        return None

    text_lower = normalized_text.lower()
    quote_lower = normalized_quote.lower()
    quote_length = len(quote_lower)
    padding = max(16, int(quote_length * 0.25))

    candidates = _candidate_starts(text_lower, quote_lower)
    if not candidates:
        step = max(1, quote_length // 4)
        candidates = list(range(0, max(1, len(text_lower) - quote_length + 1), step))

    best_ratio = 0.0
    best_span: tuple[int, int] | None = None
    seen: set[tuple[int, int]] = set()
    for start in candidates:
        candidate_start = max(0, start - padding)
        candidate_end = min(len(text_lower), start + quote_length + padding)
        if candidate_end <= candidate_start:
            continue
        span_key = (candidate_start, candidate_end)
        if span_key in seen:
            continue
        seen.add(span_key)
        candidate = text_lower[candidate_start:candidate_end]
        matcher = SequenceMatcher(None, quote_lower, candidate, autojunk=False)
        ratio = matcher.ratio()
        if ratio <= best_ratio:
            continue
        blocks = [block for block in matcher.get_matching_blocks() if block.size > 0]
        if blocks:
            start_offset = min(block.b for block in blocks)
            end_offset = max(block.b + block.size for block in blocks)
            best_span = (candidate_start + start_offset, candidate_start + end_offset)
        else:
            best_span = (candidate_start, candidate_end)
        best_ratio = ratio

    if best_span is None or best_ratio < threshold:
        return None
    normalized_start, normalized_end = best_span
    normalized_start = max(0, min(normalized_start, len(mapping) - 1))
    normalized_end = max(normalized_start + 1, min(normalized_end, len(mapping)))
    return mapping[normalized_start], mapping[normalized_end - 1] + 1


def _candidate_starts(text_lower: str, quote_lower: str) -> list[int]:
    words = [word for word in re.findall(r"[a-z0-9]+", quote_lower) if len(word) >= 4]
    anchors = []
    for word in words[:2] + words[-2:]:
        if word not in anchors:
            anchors.append(word)

    starts: set[int] = set()
    for anchor in anchors:
        position = text_lower.find(anchor)
        while position >= 0:
            starts.add(position)
            position = text_lower.find(anchor, position + 1)
    return sorted(starts)

