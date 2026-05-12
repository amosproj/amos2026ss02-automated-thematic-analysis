import io
import json

import pytest

from app.exceptions import UnprocessableError
from app.services.upload_parsers import (
    parse_docx_upload,
    parse_jsonl_upload,
    parse_pdf_upload,
    parse_txt_upload,
    parse_upload,
)


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


# ---------------------------------------------------------------------------
# .txt parser
# ---------------------------------------------------------------------------


def test_txt_one_document_with_filename_title():
    docs = parse_txt_upload("interview.txt", b"Hello world, this is a transcript.")
    assert len(docs) == 1
    assert docs[0].title == "interview.txt"
    assert "transcript" in docs[0].text


def test_txt_empty_file_raises():
    with pytest.raises(UnprocessableError):
        parse_txt_upload("f.txt", b"   \n  ")


def test_txt_invalid_encoding_raises():
    with pytest.raises(UnprocessableError):
        parse_txt_upload("f.txt", b"\xff\xfe bad bytes")


def test_txt_special_characters_in_filename_preserved():
    docs = parse_txt_upload("inteŕview-2025_v1 (final).txt", b"content")
    assert docs[0].title == "inteŕview-2025_v1 (final).txt"


# ---------------------------------------------------------------------------
# .docx parser
# ---------------------------------------------------------------------------


def _make_docx_bytes(paragraphs: list[str]) -> bytes:
    from docx import Document
    doc = Document()
    for p in paragraphs:
        doc.add_paragraph(p)
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


def test_docx_extracts_paragraphs():
    content = _make_docx_bytes(["First paragraph.", "Second paragraph."])
    docs = parse_docx_upload("interview.docx", content)
    assert len(docs) == 1
    assert "First paragraph." in docs[0].text
    assert "Second paragraph." in docs[0].text
    assert docs[0].title == "interview.docx"


def test_docx_empty_document_raises():
    content = _make_docx_bytes([])
    with pytest.raises(UnprocessableError):
        parse_docx_upload("empty.docx", content)


def test_docx_corrupted_file_raises():
    with pytest.raises(UnprocessableError):
        parse_docx_upload("bad.docx", b"not a real docx")


# ---------------------------------------------------------------------------
# .pdf parser
# ---------------------------------------------------------------------------


def _make_pdf_bytes(text: str) -> bytes:
    """Build a minimal PDF with one page of `text` using pypdf."""
    from pypdf import PdfWriter
    from pypdf.generic import (
        ArrayObject,
        DecodedStreamObject,
        DictionaryObject,
        FloatObject,
        NameObject,
        NumberObject,
    )

    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    page = writer.pages[0]
    stream = DecodedStreamObject()
    safe = text.replace("(", r"\(").replace(")", r"\)")
    stream.set_data(f"BT /F1 12 Tf 72 720 Td ({safe}) Tj ET".encode())
    page[NameObject("/Contents")] = stream
    font = DictionaryObject({
        NameObject("/Type"): NameObject("/Font"),
        NameObject("/Subtype"): NameObject("/Type1"),
        NameObject("/BaseFont"): NameObject("/Helvetica"),
    })
    page[NameObject("/Resources")] = DictionaryObject({
        NameObject("/Font"): DictionaryObject({NameObject("/F1"): font}),
    })
    page[NameObject("/MediaBox")] = ArrayObject(
        [NumberObject(0), NumberObject(0), FloatObject(612), FloatObject(792)]
    )
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def test_pdf_extracts_text():
    content = _make_pdf_bytes("Interview transcript content here")
    docs = parse_pdf_upload("interview.pdf", content)
    assert len(docs) == 1
    assert "Interview" in docs[0].text or "transcript" in docs[0].text
    assert docs[0].title == "interview.pdf"


def test_pdf_corrupted_file_raises():
    with pytest.raises(UnprocessableError):
        parse_pdf_upload("bad.pdf", b"not a real pdf")


# ---------------------------------------------------------------------------
# dispatcher
# ---------------------------------------------------------------------------


def test_parse_upload_dispatches_by_extension():
    docs = parse_upload("notes.txt", b"hello")
    assert docs[0].title == "notes.txt"


def test_parse_upload_unsupported_extension_raises():
    with pytest.raises(UnprocessableError, match="Unsupported"):
        parse_upload("data.csv", b"a,b\n1,2")


def test_parse_upload_no_extension_raises():
    with pytest.raises(UnprocessableError, match="Unsupported"):
        parse_upload("README", b"hello")


def test_parse_upload_extension_is_case_insensitive():
    docs = parse_upload("NOTES.TXT", b"hello")
    assert docs[0].title == "NOTES.TXT"
