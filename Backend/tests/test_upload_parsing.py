import json

import pytest

from app.exceptions import UnprocessableError
from app.services.ingestion import parse_jsonl_upload


# ---------------------------------------------------------------------------
# .jsonl parser
# ---------------------------------------------------------------------------


def _make_jsonl(*records: dict) -> bytes:
    return "\n".join(json.dumps(r) for r in records).encode("utf-8")


_SAMPLE = [
    {"timestamp": 1.0, "event_type": "chatbot_response", "message_index": 1, "message_content": "How are you?", "username": "p001"},
    {"timestamp": 2.0, "event_type": "human_response",   "message_index": 2, "message_content": "I am fine.",   "username": "p001"},
    {"timestamp": 3.0, "event_type": "chatbot_response", "message_index": 3, "message_content": "Tell me more.", "username": "p001"},
    {"timestamp": 4.0, "event_type": "human_response",   "message_index": 4, "message_content": "Not much else.", "username": "p001"},
    {"timestamp": 5.0, "event_type": "human_response",   "message_index": 2, "message_content": "Doing well.",   "username": "p002"},
]


def test_jsonl_one_document_per_participant():
    docs = parse_jsonl_upload("interview.jsonl", _make_jsonl(*_SAMPLE))
    titles = {d.title for d in docs}
    assert titles == {"p001", "p002"}
    assert len(docs) == 2


def test_jsonl_text_contains_only_human_responses():
    docs = parse_jsonl_upload("interview.jsonl", _make_jsonl(*_SAMPLE))
    p001 = next(d for d in docs if d.title == "p001")
    assert "I am fine." in p001.text
    assert "Not much else." in p001.text
    assert "How are you?" not in p001.text
    assert "Tell me more." not in p001.text


def test_jsonl_messages_sorted_by_index():
    shuffled = [_SAMPLE[3], _SAMPLE[1], _SAMPLE[0], _SAMPLE[2]]
    docs = parse_jsonl_upload("interview.jsonl", _make_jsonl(*shuffled))
    p001 = next(d for d in docs if d.title == "p001")
    assert p001.text.index("I am fine.") < p001.text.index("Not much else.")


def test_jsonl_skips_participants_with_no_human_responses():
    records = [
        {"event_type": "chatbot_response", "message_index": 1, "message_content": "Hi", "username": "bot_only"},
    ]
    docs = parse_jsonl_upload("f.jsonl", _make_jsonl(*records))
    assert docs == []


def test_jsonl_skips_blank_human_response_content():
    records = [
        {"event_type": "human_response", "message_index": 1, "message_content": "   ", "username": "p001"},
        {"event_type": "human_response", "message_index": 2, "message_content": "Real answer.", "username": "p001"},
    ]
    docs = parse_jsonl_upload("f.jsonl", _make_jsonl(*records))
    assert len(docs) == 1
    assert "Real answer." in docs[0].text


def test_jsonl_missing_username_raises():
    records = [{"event_type": "human_response", "message_content": "hello", "message_index": 1}]
    with pytest.raises(UnprocessableError, match="username"):
        parse_jsonl_upload("f.jsonl", _make_jsonl(*records))


def test_jsonl_invalid_json_line_raises():
    content = b'{"username": "p001", "event_type": "human_response", "message_content": "ok", "message_index": 1}\nnot json'
    with pytest.raises(UnprocessableError, match="line 2"):
        parse_jsonl_upload("f.jsonl", content)


def test_jsonl_empty_file_raises():
    with pytest.raises(UnprocessableError):
        parse_jsonl_upload("f.jsonl", b"")


def test_jsonl_invalid_encoding_raises():
    with pytest.raises(UnprocessableError):
        parse_jsonl_upload("f.jsonl", b"\xff\xfe bad bytes")
