"""CSV parser for researcher-uploaded codebooks.

Accepts raw bytes (UTF-8, optionally with BOM), validates structure,
and returns a list of ThemeInput objects ready for persistence.
"""
from __future__ import annotations

import csv
import io

from app.exceptions import UnprocessableError
from app.schemas.codebook import MAX_THEMES, MIN_THEMES, NodeInput, NodeType

# Columns that every codebook CSV must contain (matched case-insensitively after stripping).
REQUIRED_COLUMNS = {"node type", "name", "description", "parent name"}


def parse_codebook_csv(content: bytes) -> list[NodeInput]:
    """Parse a codebook CSV file from raw bytes.

    Args:
        content: Raw file bytes — UTF-8 with or without BOM.

    Returns:
        A list of NodeInput objects (guaranteed 1 ≤ len ≤ 50).

    Raises:
        UnprocessableError: If the file cannot be decoded, columns are missing,
            any theme name is blank, or the row count is out of range.
    """
    # ------------------------------------------------------------------ decode
    try:
        text = content.decode("utf-8-sig")  # utf-8-sig strips BOM if present
    except (UnicodeDecodeError, ValueError) as exc:
        raise UnprocessableError(
            f"Codebook file could not be decoded as UTF-8: {exc}"
        ) from exc

    # ------------------------------------------------------------------ parse
    reader = csv.DictReader(io.StringIO(text))

    # DictReader.fieldnames is None if the file is completely empty (0 bytes
    # after stripping BOM).  Access it once to trigger the first read.
    raw_fieldnames = reader.fieldnames
    if not raw_fieldnames:
        raise UnprocessableError(
            "Codebook CSV appears to be empty or has no header row."
        )

    # Normalise column names: lowercase + strip whitespace
    normalised: dict[str, str] = {col.strip().lower(): col for col in raw_fieldnames}

    missing = REQUIRED_COLUMNS - normalised.keys()
    if missing:
        raise UnprocessableError(
            f"Codebook CSV is missing required column(s): {', '.join(sorted(missing))}. "
            f"Found: {', '.join(sorted(normalised.keys()))}."
        )

    node_type_col = normalised["node type"]
    name_col = normalised["name"]
    description_col = normalised["description"]
    parent_name_col = normalised["parent name"]

    # ----------------------------------------------------------------- collect
    nodes: list[NodeInput] = []
    seen_names: set[str] = set()

    for row_number, raw_row in enumerate(reader, start=2):  # row 1 is the header
        node_type_val = (raw_row.get(node_type_col) or "").strip().upper()
        name_value = (raw_row.get(name_col) or "").strip()
        description_value = (raw_row.get(description_col) or "").strip()
        parent_name_value = (raw_row.get(parent_name_col) or "").strip()

        if not name_value:
            raise UnprocessableError(
                f"Row {row_number}: theme 'name' must not be empty."
            )

        try:
            node_type = NodeType(node_type_val)
        except ValueError:
            raise UnprocessableError(
                f"Row {row_number}: 'node type' must be one of THEME, SUBTHEME, CODE; got '{node_type_val}'."
            ) from None

        if node_type in (NodeType.THEME, NodeType.SUBTHEME):
            node_type = NodeType.SUBTHEME if parent_name_value else NodeType.THEME

        nodes.append(NodeInput(
            node_type=node_type,
            name=name_value,
            description=description_value or " ",
            parent_name=parent_name_value if parent_name_value else None
        ))
        seen_names.add(name_value)

    # ----------------------------------------------------------------- hierarchy validation
    for row_number, t in enumerate(nodes, start=2):
        if t.parent_name and t.parent_name not in seen_names:
            raise UnprocessableError(
                f"Row {row_number}: parent '{t.parent_name}' does not exist in the CSV."
            )

    # ----------------------------------------------------------------- count
    if not (MIN_THEMES <= len(nodes) <= MAX_THEMES):
        raise UnprocessableError(
            f"Codebook must contain between {MIN_THEMES} and {MAX_THEMES} nodes; "
            f"found {len(nodes)}."
        )

    return nodes
