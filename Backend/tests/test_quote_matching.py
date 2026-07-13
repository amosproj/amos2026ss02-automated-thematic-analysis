from app.services.quote_matching import (
    QuoteSpanCandidate,
    locate_quote_span,
    merge_quote_spans,
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


_MERGE_TRANSCRIPT = "The manual handoffs slow everyone down considerably today."


def _candidate(
    group_key: object = "code-1",
    quote: str = "manual handoffs",
    start_char: int | None = 4,
    end_char: int | None = 19,
    confidence: float = 0.9,
) -> QuoteSpanCandidate:
    return QuoteSpanCandidate(
        group_key=group_key,
        quote=quote,
        start_char=start_char,
        end_char=end_char,
        confidence=confidence,
    )


def test_merge_collapses_identical_span_preferring_confidence() -> None:
    merged = merge_quote_spans(
        [_candidate(confidence=0.6), _candidate(confidence=0.9)],
        transcript=_MERGE_TRANSCRIPT,
    )

    assert len(merged) == 1
    assert merged[0].merged is True
    assert merged[0].primary_index == 1  # the higher-confidence source
    assert set(merged[0].source_indices) == {0, 1}
    assert merged[0].quote == "manual handoffs"


def test_merge_unions_contained_span_into_container() -> None:
    merged = merge_quote_spans(
        [
            _candidate(start_char=4, end_char=24, confidence=0.5),  # "manual handoffs slow"
            _candidate(start_char=11, end_char=19, confidence=0.99),  # "handoffs"
        ],
        transcript=_MERGE_TRANSCRIPT,
    )

    assert len(merged) == 1
    assert (merged[0].start_char, merged[0].end_char) == (4, 24)
    assert merged[0].quote == "manual handoffs slow"


def test_merge_unions_partial_overlap_keeping_both_sections() -> None:
    merged = merge_quote_spans(
        [
            _candidate(start_char=4, end_char=19),  # "manual handoffs"
            _candidate(start_char=11, end_char=24),  # "handoffs slow"
        ],
        transcript=_MERGE_TRANSCRIPT,
    )

    # The distinct leading ("manual") and trailing ("slow") sections are both
    # preserved in the unioned span.
    assert len(merged) == 1
    assert (merged[0].start_char, merged[0].end_char) == (4, 24)
    assert merged[0].quote == "manual handoffs slow"


def test_merge_keeps_disjoint_spans_of_same_group() -> None:
    merged = merge_quote_spans(
        [
            _candidate(quote="The", start_char=0, end_char=3),
            _candidate(quote="everyone", start_char=25, end_char=33),
        ],
        transcript=_MERGE_TRANSCRIPT,
    )

    assert [(m.start_char, m.end_char) for m in merged] == [(0, 3), (25, 33)]
    assert all(m.merged is False for m in merged)


def test_merge_keeps_identical_span_across_different_groups() -> None:
    merged = merge_quote_spans(
        [_candidate(group_key="code-1"), _candidate(group_key="code-2")],
        transcript=_MERGE_TRANSCRIPT,
    )

    assert {m.primary_index for m in merged} == {0, 1}
    assert all(m.merged is False for m in merged)


def test_merge_deduplicates_unlocated_quotes_by_text() -> None:
    merged = merge_quote_spans(
        [
            _candidate(quote="Manual  handoffs", start_char=None, end_char=None, confidence=0.7),
            _candidate(quote="manual handoffs", start_char=None, end_char=None, confidence=0.5),
            _candidate(quote="an unrelated remark", start_char=None, end_char=None, confidence=0.5),
        ],
        transcript=_MERGE_TRANSCRIPT,
    )

    assert [m.primary_index for m in merged] == [0, 2]
    assert all(m.merged is False for m in merged)

