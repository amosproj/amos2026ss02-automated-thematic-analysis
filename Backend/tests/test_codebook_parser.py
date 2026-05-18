"""Unit tests for app/services/codebook_parser.py.

All tests are pure — no DB, no HTTP.  The parser only takes bytes and returns
ThemeInput objects (or raises UnprocessableError).
"""
from __future__ import annotations

import pytest

from app.exceptions import UnprocessableError
from app.services.codebook_parser import parse_codebook_csv


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _csv(rows: list[dict], header: list[str] | None = None) -> bytes:
    """Build a minimal CSV byte string from a list of dicts."""
    cols = header or list(rows[0].keys()) if rows else ["name", "description"]
    lines = [",".join(cols)]
    for row in rows:
        lines.append(",".join(str(row.get(c, "")) for c in cols))
    return "\n".join(lines).encode("utf-8")


def _valid_row(n: int = 1) -> dict:
    return {"name": f"Theme {n}", "description": f"Description of theme {n}"}


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_parse_valid_five_themes():
    data = _csv([_valid_row(i) for i in range(1, 6)])
    themes = parse_codebook_csv(data)
    assert len(themes) == 5
    assert themes[0].name == "Theme 1"
    assert themes[0].description == "Description of theme 1"


def test_parse_valid_fifty_themes():
    data = _csv([_valid_row(i) for i in range(1, 51)])
    themes = parse_codebook_csv(data)
    assert len(themes) == 50


def test_parse_single_theme():
    data = _csv([_valid_row(1)])
    themes = parse_codebook_csv(data)
    assert len(themes) == 1


def test_extra_columns_are_ignored():
    rows = [{"name": "T", "description": "D", "extra_col": "ignored"}]
    data = _csv(rows, header=["name", "description", "extra_col"])
    themes = parse_codebook_csv(data)
    assert len(themes) == 1
    assert themes[0].name == "T"


def test_bom_utf8_is_handled():
    """Files saved by Excel often include a UTF-8 BOM (\ufeff)."""
    raw = "name,description\nTheme A,Description A\n"
    bom_bytes = b"\xef\xbb\xbf" + raw.encode("utf-8")
    themes = parse_codebook_csv(bom_bytes)
    assert len(themes) == 1
    assert themes[0].name == "Theme A"


def test_column_names_case_insensitive():
    data = b"Name,Description\nInterpretation,How we interpret it\n"
    themes = parse_codebook_csv(data)
    assert themes[0].name == "Interpretation"


def test_column_names_with_surrounding_whitespace():
    data = b" name , description \nSafety,Relates to safety\n"
    themes = parse_codebook_csv(data)
    assert themes[0].name == "Safety"


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


def test_too_many_themes_raises():
    data = _csv([_valid_row(i) for i in range(1, 52)])  # 51 rows
    with pytest.raises(UnprocessableError, match="51"):
        parse_codebook_csv(data)


def test_zero_themes_raises():
    data = b"name,description\n"  # header only
    with pytest.raises(UnprocessableError, match="0"):
        parse_codebook_csv(data)


def test_missing_name_column_raises():
    data = b"label,description\nTheme A,Desc A\n"
    with pytest.raises(UnprocessableError, match="name"):
        parse_codebook_csv(data)


def test_missing_description_column_raises():
    data = b"name,desc\nTheme A,Desc A\n"
    with pytest.raises(UnprocessableError, match="description"):
        parse_codebook_csv(data)


def test_blank_name_in_row_raises():
    data = b"name,description\n,Some description\n"
    with pytest.raises(UnprocessableError, match="Row 2"):
        parse_codebook_csv(data)


def test_empty_file_raises():
    with pytest.raises(UnprocessableError, match="empty"):
        parse_codebook_csv(b"")


def test_binary_content_raises():
    with pytest.raises(UnprocessableError):
        parse_codebook_csv(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")
