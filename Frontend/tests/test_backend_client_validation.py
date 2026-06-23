"""Unit tests for _parse_validation_detail in the backend client.

Focus: the message a 4xx error surfaces to the user. Our backend wraps handled
errors in an envelope where the human-readable message can live in `detail`,
`meta.detail`, or the top-level `error` field — the parser must find it in all
of these so service-raised messages aren't lost to the generic fallback.
"""
from __future__ import annotations

import httpx

from web.services.backend_client import _parse_validation_detail


def _resp(payload: dict) -> httpx.Response:
    return httpx.Response(status_code=422, json=payload)


def test_parses_fastapi_list_detail() -> None:
    payload = {"detail": [{"loc": ["body", "provider"], "msg": "field required"}]}
    assert _parse_validation_detail(_resp(payload)) == "provider: field required"


def test_parses_string_detail() -> None:
    assert _parse_validation_detail(_resp({"detail": "bad thing"})) == "bad thing"


def test_parses_meta_detail() -> None:
    payload = {"success": False, "error": "Validation failed", "meta": {"detail": "field x is wrong"}}
    assert _parse_validation_detail(_resp(payload)) == "field x is wrong"


def test_falls_back_to_top_level_error() -> None:
    # Service-raised UnprocessableError envelope: message is in `error`, meta null.
    payload = {
        "success": False,
        "error": "Academic Cloud has no API key configured on the server.",
        "meta": None,
    }
    assert (
        _parse_validation_detail(_resp(payload))
        == "Academic Cloud has no API key configured on the server."
    )


def test_returns_none_when_no_message() -> None:
    assert _parse_validation_detail(_resp({"success": False, "meta": None})) is None
