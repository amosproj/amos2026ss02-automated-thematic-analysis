"""File parsers for the upload endpoint.

Each parser converts raw bytes from an uploaded file into a list of
DocumentInput objects. The dispatcher `parse_upload` picks the right parser
based on file extension. Adding a new format = one parser function + one
entry in `_PARSERS`.
"""

from __future__ import annotations

import io
import json
from collections.abc import Callable

from app.exceptions import UnprocessableError
from app.schemas.ingestion import DocumentInput


def _decode_utf8(filename: str, content: bytes) -> str:
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UnprocessableError(f"Could not decode '{filename}' as UTF-8") from exc


def parse_txt_upload(filename: str, content: bytes) -> list[DocumentInput]:
    """One document per .txt file. Title falls back to the filename."""
    text = _decode_utf8(filename, content).strip()
    if not text:
        raise UnprocessableError(f"'{filename}': file is empty")
    return [DocumentInput(title=filename, text=text)]


def parse_docx_upload(filename: str, content: bytes) -> list[DocumentInput]:
    """One document per .docx file. Concatenates all paragraph text."""
    from docx import Document  # type: ignore[import-not-found]

    try:
        doc = Document(io.BytesIO(content))
    except Exception as exc:
        raise UnprocessableError(f"'{filename}': could not read .docx file") from exc

    text = "\n\n".join(p.text for p in doc.paragraphs if p.text.strip()).strip()
    if not text:
        raise UnprocessableError(f"'{filename}': document contains no text")
    return [DocumentInput(title=filename, text=text)]


def parse_pdf_upload(filename: str, content: bytes) -> list[DocumentInput]:
    """One document per .pdf file. Concatenates extracted text from each page."""
    from pypdf import PdfReader  # type: ignore[import-not-found]

    try:
        reader = PdfReader(io.BytesIO(content))
        pages_text = [page.extract_text() or "" for page in reader.pages]
    except Exception as exc:
        raise UnprocessableError(f"'{filename}': could not read .pdf file") from exc

    text = "\n\n".join(t.strip() for t in pages_text if t.strip()).strip()
    if not text:
        raise UnprocessableError(f"'{filename}': document contains no extractable text")
    return [DocumentInput(title=filename, text=text)]


def parse_jsonl_upload(filename: str, content: bytes) -> list[DocumentInput]:
    """One DocumentInput per username. Keeps only `human_response` events, sorted
    by `message_index`. Participants with no non-blank human turns are skipped."""
    text_content = _decode_utf8(filename, content)

    participants: dict[str, list[dict]] = {}
    for line_no, raw in enumerate(text_content.splitlines(), start=1):
        raw = raw.strip()
        if not raw:
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise UnprocessableError(f"'{filename}': invalid JSON on line {line_no}: {exc}") from exc

        username = record.get("username")
        if not username:
            raise UnprocessableError(f"'{filename}': line {line_no} is missing 'username'")
        participants.setdefault(username, []).append(record)

    if not participants:
        raise UnprocessableError(f"'{filename}': file contains no records")

    docs: list[DocumentInput] = []
    for username, messages in participants.items():
        messages.sort(key=lambda m: m.get("message_index", 0))
        human_turns = [
            m for m in messages
            if m.get("event_type") == "human_response"
            and str(m.get("message_content", "")).strip()
        ]
        if not human_turns:
            continue
        text = "\n\n".join(str(m["message_content"]) for m in human_turns)
        docs.append(DocumentInput(title=username, text=text))
    return docs


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


ParserFn = Callable[[str, bytes], list[DocumentInput]]

_PARSERS: dict[str, ParserFn] = {
    ".txt": parse_txt_upload,
    ".docx": parse_docx_upload,
    ".pdf": parse_pdf_upload,
    ".jsonl": parse_jsonl_upload,
}

SUPPORTED_EXTENSIONS = frozenset(_PARSERS.keys())


def get_extension(filename: str) -> str:
    """Return the lowercase file extension including the dot, or '' if none."""
    dot = filename.rfind(".")
    return filename[dot:].lower() if dot != -1 else ""


def parse_upload(filename: str, content: bytes) -> list[DocumentInput]:
    """Dispatch to the right parser by file extension. Raises UnprocessableError
    if the extension is unsupported."""
    ext = get_extension(filename)
    parser = _PARSERS.get(ext)
    if parser is None:
        raise UnprocessableError(
            f"Unsupported file extension '{ext}'. "
            f"Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        )
    return parser(filename, content)
