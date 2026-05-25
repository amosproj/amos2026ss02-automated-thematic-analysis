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
    cols = header or list(rows[0].keys()) if rows else ["node type", "name", "description", "parent name"]
    lines = [",".join(cols)]
    for row in rows:
        lines.append(",".join(str(row.get(c, "")) for c in cols))
    return "\n".join(lines).encode("utf-8")


def _valid_row(n: int = 1) -> dict:
    return {
        "node type": "THEME", 
        "name": f"Theme {n}", 
        "description": f"Description of theme {n}",
        "parent name": ""
    }


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
    rows = [{"node type": "THEME", "name": "T", "description": "D", "parent name": "", "extra_col": "ignored"}]
    data = _csv(rows, header=["node type", "name", "description", "parent name", "extra_col"])
    themes = parse_codebook_csv(data)
    assert len(themes) == 1
    assert themes[0].name == "T"


def test_bom_utf8_is_handled():
    """Files saved by Excel often include a UTF-8 BOM (\ufeff)."""
    raw = "node type,name,description,parent name\nTHEME,Theme A,Description A,\n"
    bom_bytes = b"\xef\xbb\xbf" + raw.encode("utf-8")
    themes = parse_codebook_csv(bom_bytes)
    assert len(themes) == 1
    assert themes[0].name == "Theme A"


def test_column_names_case_insensitive():
    data = b"Node Type,Name,Description,Parent Name\nTHEME,Interpretation,How we interpret it,\n"
    themes = parse_codebook_csv(data)
    assert themes[0].name == "Interpretation"


def test_column_names_with_surrounding_whitespace():
    data = b" node type , name , description , parent name \n THEME , Safety,Relates to safety, \n"
    themes = parse_codebook_csv(data)
    assert themes[0].name == "Safety"

def test_valid_hierarchy():
    data = _csv([
        {"node type": "THEME", "name": "Theme A", "description": "Desc A", "parent name": ""},
        {"node type": "SUBTHEME", "name": "Sub A1", "description": "Desc A1", "parent name": "Theme A"},
        {"node type": "CODE", "name": "Code A1", "description": "Desc C", "parent name": "Sub A1"}
    ])
    themes = parse_codebook_csv(data)
    assert len(themes) == 3
    assert themes[1].parent_name == "Theme A"
    assert themes[2].parent_name == "Sub A1"


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


def test_too_many_themes_raises():
    data = _csv([_valid_row(i) for i in range(1, 52)])  # 51 rows
    with pytest.raises(UnprocessableError, match="51"):
        parse_codebook_csv(data)


def test_zero_themes_raises():
    data = b"node type,name,description,parent name\n"  # header only
    with pytest.raises(UnprocessableError, match="0"):
        parse_codebook_csv(data)


def test_missing_name_column_raises():
    data = b"node type,label,description,parent name\nTHEME,Theme A,Desc A,\n"
    with pytest.raises(UnprocessableError, match="name"):
        parse_codebook_csv(data)


def test_missing_description_column_raises():
    data = b"node type,name,desc,parent name\nTHEME,Theme A,Desc A,\n"
    with pytest.raises(UnprocessableError, match="description"):
        parse_codebook_csv(data)


def test_blank_name_in_row_raises():
    data = b"node type,name,description,parent name\nTHEME,,Some description,\n"
    with pytest.raises(UnprocessableError, match="Row 2"):
        parse_codebook_csv(data)

def test_invalid_node_type_raises():
    data = _csv([{"node type": "INVALID", "name": "A", "description": "B", "parent name": ""}])
    with pytest.raises(UnprocessableError, match="one of THEME, SUBTHEME, CODE"):
        parse_codebook_csv(data)

def test_missing_parent_name_raises():
    data = _csv([{"node type": "SUBTHEME", "name": "A", "description": "B", "parent name": ""}])
    with pytest.raises(UnprocessableError, match="must have a 'parent name'"):
        parse_codebook_csv(data)

def test_theme_with_parent_name_raises():
    data = _csv([{"node type": "THEME", "name": "A", "description": "B", "parent name": "B"}])
    with pytest.raises(UnprocessableError, match="'THEME' must not have a 'parent name'"):
        parse_codebook_csv(data)

def test_missing_parent_in_csv_raises():
    data = _csv([{"node type": "SUBTHEME", "name": "A", "description": "B", "parent name": "Nonexistent"}])
    with pytest.raises(UnprocessableError, match="parent 'Nonexistent' does not exist in the CSV"):
        parse_codebook_csv(data)


def test_empty_file_raises():
    with pytest.raises(UnprocessableError, match="empty"):
        parse_codebook_csv(b"")


def test_binary_content_raises():
    with pytest.raises(UnprocessableError):
        parse_codebook_csv(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")
