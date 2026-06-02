from __future__ import annotations

import re

# Strip entire <script>...</script> and <style>...</style> blocks including their content.
_SCRIPT_BLOCK_RE = re.compile(r"<(script|style)[^>]*>.*?</(script|style)>", re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE = re.compile(r"<[^>]*>")
_CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def sanitize_research_query(value: str) -> str:
    """Strip script/style blocks, HTML tags, and control characters from a research query.

    Normalises whitespace but preserves newlines so multi-line queries remain readable.
    Strips leading/trailing whitespace. Callers enforce min/max length after sanitisation.
    """
    cleaned = _SCRIPT_BLOCK_RE.sub("", value)
    cleaned = _HTML_TAG_RE.sub("", cleaned)
    cleaned = _CONTROL_CHAR_RE.sub("", cleaned)
    # Collapse runs of spaces/tabs on each line, then strip outer whitespace.
    lines = [" ".join(line.split()) for line in cleaned.splitlines()]
    cleaned = "\n".join(lines).strip()
    return cleaned
