import csv
import io
import json

import pytest

API = "/api/v1/ingestion"

P1_STR = "00000000-0000-0000-0000-000000000001"
P2_STR = "00000000-0000-0000-0000-000000000002"
MISSING_STR = "00000000-0000-0000-0000-000000000000"


# ---------------------------------------------------------------------------
# POST /ingestion/corpora
# ---------------------------------------------------------------------------


async def test_create_corpus_returns_201(client):
    resp = await client.post(
        f"{API}/corpora",
        json={"project_id": P1_STR, "name": "My Corpus"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["success"] is True
    assert body["data"]["name"] == "My Corpus"
    assert body["data"]["project_id"] == P1_STR
    assert "id" in body["data"]


# ---------------------------------------------------------------------------
# GET /ingestion/corpora
# ---------------------------------------------------------------------------


async def test_list_corpora_empty(client):
    resp = await client.get(f"{API}/corpora", params={"project_id": MISSING_STR})
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"]["items"] == []
    assert body["data"]["meta"]["total"] == 0


async def test_list_corpora_returns_created(client):
    await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C1"})
    await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C2"})
    await client.post(f"{API}/corpora", json={"project_id": P2_STR, "name": "C3"})

    resp = await client.get(f"{API}/corpora", params={"project_id": P1_STR})
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["meta"]["total"] == 2
    assert len(data["items"]) == 2


# ---------------------------------------------------------------------------
# GET /ingestion/corpora/{corpus_id}
# ---------------------------------------------------------------------------


async def test_get_corpus_by_id(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "X"})
    corpus_id = create.json()["data"]["id"]

    resp = await client.get(f"{API}/corpora/{corpus_id}")
    assert resp.status_code == 200
    assert resp.json()["data"]["id"] == corpus_id


async def test_get_corpus_not_found(client):
    resp = await client.get(f"{API}/corpora/{MISSING_STR}")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /ingestion/corpora/{corpus_id}/documents/bulk
# ---------------------------------------------------------------------------


async def test_bulk_ingest_documents(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    payload = {
        "documents": [
            {"title": "Doc 1", "text": "First document with some words here"},
            {"title": "Doc 2", "text": "Second document with different words"},
        ],
    }
    resp = await client.post(f"{API}/corpora/{corpus_id}/documents/bulk", json=payload)
    assert resp.status_code == 201

    data = resp.json()["data"]
    assert data["documents_created"] == 2
    assert data["chunks_created"] > 0


async def test_bulk_ingest_skips_empty_documents(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    payload = {"documents": [{"title": "E", "text": ""}, {"title": "V", "text": "valid text here"}]}
    resp = await client.post(f"{API}/corpora/{corpus_id}/documents/bulk", json=payload)
    assert resp.status_code == 201
    assert resp.json()["data"]["documents_created"] == 1


# ---------------------------------------------------------------------------
# GET /ingestion/corpora/{corpus_id}/documents
# ---------------------------------------------------------------------------


async def test_get_documents_paginated(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    docs = [{"title": f"Doc {i}", "text": f"Document number {i} with enough words"} for i in range(5)]
    await client.post(f"{API}/corpora/{corpus_id}/documents/bulk", json={"documents": docs})

    resp = await client.get(f"{API}/corpora/{corpus_id}/documents", params={"page": 1, "page_size": 3})
    assert resp.status_code == 200
    meta = resp.json()["data"]["meta"]
    assert meta["total"] == 5
    assert len(resp.json()["data"]["items"]) == 3


# ---------------------------------------------------------------------------
# GET /ingestion/corpora/{corpus_id}/chunks
# ---------------------------------------------------------------------------


async def test_get_chunks(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    await client.post(
        f"{API}/corpora/{corpus_id}/documents/bulk",
        json={"documents": [{"title": "T", "text": "word " * 15}]},
    )

    resp = await client.get(f"{API}/corpora/{corpus_id}/chunks")
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["meta"]["total"] > 0
    chunk = data["items"][0]
    assert "chunk_index" in chunk
    assert "text" in chunk


# ---------------------------------------------------------------------------
# POST /ingestion/corpora/{corpus_id}/upload — txt
# ---------------------------------------------------------------------------


async def test_upload_txt(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    content = "word " * 20
    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"file": ("sample.txt", content.encode(), "text/plain")},
    )
    assert resp.status_code == 201
    assert resp.json()["data"]["documents_created"] == 1


async def test_upload_json(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    data_bytes = json.dumps([
        {"title": "J1", "text": "first json document with multiple words"},
        {"title": "J2", "text": "second json document with multiple words"},
    ]).encode()
    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"file": ("docs.json", data_bytes, "application/json")},
    )
    assert resp.status_code == 201
    assert resp.json()["data"]["documents_created"] == 2


async def test_upload_csv(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["text", "title"])
    writer.writeheader()
    writer.writerow({"text": "csv document one with many words here now", "title": "T1"})
    writer.writerow({"text": "csv document two with many words here now", "title": "T2"})

    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"file": ("data.csv", buf.getvalue().encode(), "text/csv")},
    )
    assert resp.status_code == 201
    assert resp.json()["data"]["documents_created"] == 2


async def test_upload_unsupported_extension(client):
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"file": ("data.xml", b"<xml/>", "application/xml")},
    )
    assert resp.status_code == 422
