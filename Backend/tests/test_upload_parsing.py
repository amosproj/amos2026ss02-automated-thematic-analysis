import csv
import io
import json

import pytest

from app.exceptions import UnprocessableError
from app.services.ingestion import parse_csv_upload, parse_json_upload, parse_jsonl_upload, parse_text_upload


# ---------------------------------------------------------------------------
# .txt parser
# ---------------------------------------------------------------------------


def test_txt_creates_one_document():
    content = b"Hello world, this is a test document."
    docs = parse_text_upload("sample.txt", content)
    assert len(docs) == 1
    assert docs[0].text == "Hello world, this is a test document."
    assert docs[0].title == "sample.txt"


def test_txt_preserves_full_content():
    content = "Line one\nLine two\nLine three".encode("utf-8")
    docs = parse_text_upload("doc.txt", content)
    assert "Line one" in docs[0].text
    assert "Line three" in docs[0].text


def test_txt_invalid_encoding_raises():
    with pytest.raises(UnprocessableError):
        parse_text_upload("bad.txt", b"\xff\xfe invalid utf-8 \x80")


# ---------------------------------------------------------------------------
# .json parser
# ---------------------------------------------------------------------------


def test_json_list_format():
    data = [
        {"text": "Document one", "title": "T1"},
        {"text": "Document two"},
    ]
    docs = parse_json_upload("data.json", json.dumps(data).encode())
    assert len(docs) == 2
    assert docs[0].text == "Document one"
    assert docs[0].title == "T1"
    assert docs[1].title == "data.json"  # fallback to filename


def test_json_object_with_documents_key():
    data = {"documents": [{"text": "Hello"}, {"text": "World"}]}
    docs = parse_json_upload("data.json", json.dumps(data).encode())
    assert len(docs) == 2


def test_json_missing_text_raises():
    data = [{"title": "no text here"}]
    with pytest.raises(UnprocessableError, match="text"):
        parse_json_upload("bad.json", json.dumps(data).encode())


def test_json_invalid_json_raises():
    with pytest.raises(UnprocessableError):
        parse_json_upload("bad.json", b"{not valid json}")


def test_json_wrong_structure_raises():
    with pytest.raises(UnprocessableError):
        parse_json_upload("bad.json", json.dumps({"no_docs_key": []}).encode())


def test_json_item_not_dict_raises():
    with pytest.raises(UnprocessableError):
        parse_json_upload("bad.json", json.dumps(["just a string"]).encode())


# ---------------------------------------------------------------------------
# .csv parser
# ---------------------------------------------------------------------------


def _make_csv(rows: list[dict], fieldnames: list[str] | None = None) -> bytes:
    buf = io.StringIO()
    fields = fieldnames or list(rows[0].keys())
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def test_csv_basic():
    content = _make_csv([{"text": "Hello world", "title": "T"}])
    docs = parse_csv_upload("data.csv", content)
    assert len(docs) == 1
    assert docs[0].text == "Hello world"
    assert docs[0].title == "T"


def test_csv_title_falls_back_to_filename_and_row():
    content = _make_csv([{"text": "hi"}])
    docs = parse_csv_upload("data.csv", content)
    assert docs[0].title == "data.csv:1"


def test_csv_missing_text_column_raises():
    content = _make_csv([{"title": "no text"}])
    with pytest.raises(UnprocessableError, match="text"):
        parse_csv_upload("bad.csv", content)


def test_csv_skips_empty_text_rows():
    rows = [{"text": "valid document"}, {"text": ""}, {"text": "   "}]
    docs = parse_csv_upload("data.csv", _make_csv(rows))
    assert len(docs) == 1
    assert docs[0].text == "valid document"


def test_csv_multiple_rows():
    rows = [{"text": f"Document {i}"} for i in range(5)]
    docs = parse_csv_upload("data.csv", _make_csv(rows))
    assert len(docs) == 5


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
