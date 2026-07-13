from app.services.quote_matching import (
    QuoteSpanCandidate,
    locate_quote_span,
    select_deduplicated_quote_spans,
)


def test_locate_quote_span_exact_match() -> None:
    transcript = "Participant: The manual handoffs slow everyone down."

    match = locate_quote_span(transcript, "manual handoffs slow")

    assert match.quote_match_status == "exact"
    assert transcript[match.start_char:match.end_char] == "manual handoffs slow"


def test_locate_quote_span_normalized_whitespace_match() -> None:
    transcript = "Participant: The manual\nhandoffs   slow everyone down."

    match = locate_quote_span(transcript, "manual handoffs slow")

    assert match.quote_match_status == "normalized"
    assert match.start_char is not None
    assert match.end_char is not None
    assert "manual" in transcript[match.start_char:match.end_char]
    assert "slow" in transcript[match.start_char:match.end_char]


def test_locate_quote_span_not_found() -> None:
    match = locate_quote_span("Participant: No matching excerpt here.", "absent quote")

    assert match.quote_match_status == "not_found"
    assert match.start_char is None
    assert match.end_char is None


def _candidate(
    group_key: object = "theme-1",
    quote: str = "manual handoffs slow",
    start_char: int | None = 10,
    end_char: int | None = 30,
    confidence: float = 0.9,
    quote_match_status: str = "exact",
) -> QuoteSpanCandidate:
    return QuoteSpanCandidate(
        group_key=group_key,
        quote=quote,
        start_char=start_char,
        end_char=end_char,
        confidence=confidence,
        quote_match_status=quote_match_status,
    )


def test_dedup_keeps_one_copy_of_identical_span_preferring_confidence() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(confidence=0.6),
        _candidate(confidence=0.9),
    ])

    assert kept == [1]


def test_dedup_prefers_higher_confidence_over_longer_contained_span() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(start_char=12, end_char=25, confidence=0.99),
        _candidate(start_char=10, end_char=30, confidence=0.5),
    ])

    assert kept == [0]


def test_dedup_keeps_higher_confidence_of_partially_overlapping_spans() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(start_char=10, end_char=30, confidence=0.5),
        _candidate(start_char=25, end_char=40, confidence=0.99),
    ])

    assert kept == [1]


def test_dedup_prefers_exact_match_over_higher_confidence_fuzzy() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(start_char=10, end_char=30, confidence=0.6, quote_match_status="exact"),
        _candidate(start_char=5, end_char=45, confidence=0.99, quote_match_status="fuzzy"),
    ])

    assert kept == [0]


def test_dedup_breaks_status_and_confidence_ties_by_longer_span() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(start_char=12, end_char=25),
        _candidate(start_char=10, end_char=30),
    ])

    assert kept == [1]


def test_dedup_keeps_identical_span_across_different_groups() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(group_key="theme-1"),
        _candidate(group_key="theme-2"),
    ])

    assert kept == [0, 1]


def test_dedup_keeps_non_overlapping_spans_in_same_group() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(start_char=10, end_char=30),
        _candidate(start_char=40, end_char=60),
    ])

    assert kept == [0, 1]


def test_dedup_unlocated_quotes_by_whitespace_insensitive_text() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(quote="Manual  handoffs\nslow", start_char=None, end_char=None, confidence=0.7),
        _candidate(quote="manual handoffs slow", start_char=None, end_char=None, confidence=0.5),
        _candidate(quote="an unrelated remark", start_char=None, end_char=None, confidence=0.5),
    ])

    assert kept == [0, 2]


def test_dedup_drops_unlocated_duplicate_of_kept_located_quote() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(start_char=10, end_char=30),
        _candidate(start_char=None, end_char=None, confidence=0.99),
    ])

    assert kept == [0]


def test_dedup_drops_unlocated_duplicate_of_dropped_overlapping_quote() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(quote="the manual handoffs slow everyone", start_char=10, end_char=43),
        _candidate(quote="manual handoffs slow", start_char=14, end_char=34, confidence=0.5),
        _candidate(quote="manual handoffs slow", start_char=None, end_char=None),
    ])

    assert kept == [0]


def test_dedup_treats_degenerate_spans_as_unlocated() -> None:
    kept = select_deduplicated_quote_spans([
        _candidate(start_char=10, end_char=30),
        # Zero-length span, same text as the kept row -> deduped by text.
        _candidate(quote="manual  handoffs\nslow", start_char=17, end_char=17),
        # Inverted span, unique text -> kept, but must not block anything.
        _candidate(quote="an unrelated remark", start_char=40, end_char=35),
        _candidate(quote="everyone down", start_char=36, end_char=49),
    ])

    assert kept == [0, 2, 3]

