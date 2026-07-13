from app.services.quote_matching import locate_quote_span


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

