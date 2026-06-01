"""Integration tests for the codebooks API router.

Uses the client fixture from conftest.py which provides fully integration
tested endpoints over HTTP/REST on an in-memory SQLite database.
"""
from __future__ import annotations

import uuid

API = "/api/v1/codebooks"
CORPUS_ID = "00000000-0000-0000-0000-000000000099"

INGESTION_API = "/api/v1/ingestion"


async def _ensure_corpus(client) -> str:
    """Create a corpus if it doesn't exist, return its id."""
    resp = await client.post(
        f"{INGESTION_API}/corpora",
        json={"corpus_id": CORPUS_ID, "name": "Test Corpus"},
    )
    # 201 if new, may fail if already exists — that's fine
    if resp.status_code == 201:
        return resp.json()["data"]["id"]
    return CORPUS_ID


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
# POST /codebooks/parse-csv (CSV Parser Preview Endpoint)
# ---------------------------------------------------------------------------


async def test_parse_csv_valid_five_themes(client):
    csv_bytes = _csv([_valid_row(i) for i in range(1, 6)])
    resp = await client.post(
        f"{API}/parse-csv",
        files={"file": ("codebook.csv", csv_bytes, "text/csv")},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert len(body["data"]) == 5
    assert body["data"][0]["name"] == "Theme 1"
    assert body["data"][0]["description"] == "Description of theme 1"


async def test_parse_csv_valid_fifty_themes(client):
    csv_bytes = _csv([_valid_row(i) for i in range(1, 51)])
    resp = await client.post(
        f"{API}/parse-csv",
        files={"file": ("codebook.csv", csv_bytes, "text/csv")},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert len(body["data"]) == 50


async def test_parse_csv_too_many_themes_raises_422(client):
    csv_bytes = _csv([_valid_row(i) for i in range(1, 52)])  # 51 themes
    resp = await client.post(
        f"{API}/parse-csv",
        files={"file": ("codebook.csv", csv_bytes, "text/csv")},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["success"] is False
    assert "between 1 and 50" in body["error"]


async def test_parse_csv_missing_column_raises_422(client):
    csv_bytes = b"node type,title,description,parent name\nTHEME,Theme A,Desc A,\n"  # missing 'name'
    resp = await client.post(
        f"{API}/parse-csv",
        files={"file": ("codebook.csv", csv_bytes, "text/csv")},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["success"] is False
    assert "missing required column" in body["error"]


async def test_parse_csv_malformed_binary_raises_422(client):
    resp = await client.post(
        f"{API}/parse-csv",
        files={"file": ("codebook.csv", b"\x89PNG\r\n", "text/csv")},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["success"] is False


async def test_parse_csv_zero_themes_raises_422(client):
    csv_bytes = b"node type,name,description,parent name\n"  # header only
    resp = await client.post(
        f"{API}/parse-csv",
        files={"file": ("codebook.csv", csv_bytes, "text/csv")},
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["success"] is False
    assert "between 1 and 50" in body["error"]


# ---------------------------------------------------------------------------
# POST /codebooks (Creation Endpoint)
# ---------------------------------------------------------------------------


async def test_create_codebook_valid(client):
    await _ensure_corpus(client)
    payload = {
        "name": "Research Codebook",
        "corpus_id": CORPUS_ID,
        "nodes": [
            {"name": "Theme A", "description": "Desc A", "node_type": "THEME"},
            {"name": "Theme B", "description": "Desc B", "node_type": "THEME"},
                {"name": "Theme C", "description": "Desc C", "node_type": "THEME"},
                {"name": "Theme D", "description": "Desc D", "node_type": "THEME"},
                {"name": "Theme E", "description": "Desc E", "node_type": "THEME"},
        ],
    }
    resp = await client.post(f"{API}/", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["name"] == "Research Codebook"
    assert body["data"]["corpus_id"] == CORPUS_ID
    assert body["data"]["version"] == 1
    assert len(body["data"]["themes"]) == 5
    assert body["data"]["themes"][0]["name"] == "Theme A"


async def test_create_codebook_zero_themes_raises_422(client):
    payload = {
        "name": "Research Codebook",
        "corpus_id": CORPUS_ID,
        "nodes": [],
    }
    resp = await client.post(f"{API}/", json=payload)
    assert resp.status_code == 422
    body = resp.json()
    assert body["success"] is False
    assert "validation" in body["error"].lower() or "value" in body["error"].lower()


async def test_create_codebook_too_many_themes_raises_422(client):
    payload = {
        "name": "Research Codebook",
        "corpus_id": CORPUS_ID,
        "nodes": [{"name": f"Theme {i}", "description": "Desc"} for i in range(51)],
    }
    resp = await client.post(f"{API}/", json=payload)
    assert resp.status_code == 422
    body = resp.json()
    assert body["success"] is False


# ---------------------------------------------------------------------------
# GET /codebooks/{codebook_id} (Detail Endpoint)
# ---------------------------------------------------------------------------


async def test_get_codebook_detail_success(client):
    await _ensure_corpus(client)
    # First create a codebook
    payload = {
        "name": "Read Codebook",
        "corpus_id": CORPUS_ID,
        "nodes": [{"name": "A", "description": "Desc A", "node_type": "THEME"}, {"name": "B", "description": "Desc B", "node_type": "THEME"}, {"name": "C", "description": "Desc C", "node_type": "THEME"}, {"name": "D", "description": "Desc D", "node_type": "THEME"}, {"name": "E", "description": "Desc E", "node_type": "THEME"}],
    }
    create_resp = await client.post(f"{API}/", json=payload)
    cb_id = create_resp.json()["data"]["id"]

    # Now get it
    resp = await client.get(f"{API}/{cb_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["id"] == cb_id
    assert len(body["data"]["themes"]) == 5


async def test_get_codebook_detail_not_found(client):
    unknown_id = str(uuid.uuid4())
    resp = await client.get(f"{API}/{unknown_id}")
    assert resp.status_code == 404
    body = resp.json()
    assert body["success"] is False
    assert "not found" in body["error"].lower()


# ---------------------------------------------------------------------------
# GET /codebooks (List Endpoint — Regression Check)
# ---------------------------------------------------------------------------


async def test_list_codebooks_empty(client):
    resp = await client.get(f"{API}/?corpus_id={CORPUS_ID}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert isinstance(body["data"], list)
