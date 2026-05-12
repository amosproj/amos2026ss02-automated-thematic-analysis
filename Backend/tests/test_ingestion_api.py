from pathlib import Path

import pytest

API = "/api/v1/ingestion"

# Synthesized content — long enough to substantively exercise the chunker.
_LONG_TEXT = (
    "The participant described their daily workflow in some detail. "
    "They mentioned working with contracts, performing document review, "
    "and using a variety of tools throughout the day. The interview "
    "covered topics including efficiency, accuracy, ethical considerations, "
    "training requirements, and the future of the profession. Overall the "
    "conversation provided a substantive view of the participant's experience."
).encode()

# Real-transcript fixtures for the *_real_fixture tests below.
_DATA_DIR = Path(__file__).resolve().parent / "test-data"
_TXT_FIXTURE = _DATA_DIR / "test_interview.txt"
_DOCX_FIXTURE = _DATA_DIR / "test_interview.docx"
_PDF_FIXTURE = _DATA_DIR / "test_interview.pdf"
_JSONL_FIXTURE = _DATA_DIR / "test_interview.jsonl"

P1_STR = "00000000-0000-0000-0000-000000000001"
P2_STR = "00000000-0000-0000-0000-000000000002"
MISSING_STR = "00000000-0000-0000-0000-000000000000"


class _AssertionLog:
    """Collects descriptions of passing assertions and prints a numbered summary.
    Mirrors test_llm_academic_cloud.py — run pytest with `-s` to see the output."""

    def __init__(self) -> None:
        self.passed: list[str] = []

    def check(self, condition: bool, description: str) -> None:
        assert condition, description
        self.passed.append(description)

    def report(self, header: str) -> None:
        print(f"\n--- {header}: {len(self.passed)} assertion(s) passed ---")
        for i, msg in enumerate(self.passed, 1):
            print(f"  {i}. {msg}")


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
# POST /ingestion/corpora/{corpus_id}/upload — one test per format, using the
# bundled real-transcript fixtures.
# ---------------------------------------------------------------------------


async def test_upload_txt_real_fixture(client):
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    content = _TXT_FIXTURE.read_bytes()
    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": (_TXT_FIXTURE.name, content, "text/plain")},
    )
    log.check(resp.status_code == 201, f"endpoint returned 201 (got {resp.status_code})")
    result = resp.json()["data"]["results"][0]
    log.check(result["success"] is True, f"per-file success=True for {_TXT_FIXTURE.name}")
    log.check(result["documents_created"] == 1, "exactly one document created")
    log.check(result["chunks_created"] >= 1, f"at least one chunk created ({result['chunks_created']})")
    log.report(f"Upload .txt real fixture [{_TXT_FIXTURE.name}, {len(content)} bytes]")


async def test_upload_docx_real_fixture(client):
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    content = _DOCX_FIXTURE.read_bytes()
    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": (
            _DOCX_FIXTURE.name, content,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )},
    )
    log.check(resp.status_code == 201, f"endpoint returned 201 (got {resp.status_code})")
    result = resp.json()["data"]["results"][0]
    log.check(result["success"] is True, f"per-file success=True for {_DOCX_FIXTURE.name}")
    log.check(result["documents_created"] == 1, "exactly one document created")
    log.check(result["chunks_created"] >= 1, f"at least one chunk created ({result['chunks_created']})")
    log.report(f"Upload .docx real fixture [{_DOCX_FIXTURE.name}, {len(content)} bytes]")


async def test_upload_pdf_real_fixture(client):
    """Also verifies the full upload → chunks pipeline preserves text content.
    PDF is chosen because its extraction is the most prone to silent failure."""
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    content = _PDF_FIXTURE.read_bytes()
    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": (_PDF_FIXTURE.name, content, "application/pdf")},
    )
    log.check(resp.status_code == 201, f"endpoint returned 201 (got {resp.status_code})")
    result = resp.json()["data"]["results"][0]
    log.check(result["success"] is True, f"per-file success=True for {_PDF_FIXTURE.name}")
    log.check(result["documents_created"] == 1, "exactly one document created")
    log.check(result["chunks_created"] >= 1, f"at least one chunk created ({result['chunks_created']})")

    # Fetch chunks back and confirm uploaded text reassembled into substantive content.
    chunks_resp = await client.get(
        f"{API}/corpora/{corpus_id}/chunks", params={"page_size": 1000}
    )
    reassembled = " ".join(c["text"] for c in chunks_resp.json()["data"]["items"])
    log.check(
        len(reassembled.strip()) > 200,
        f"chunks reassembled into {len(reassembled)} chars of readable text",
    )
    log.report(f"Upload .pdf real fixture [{_PDF_FIXTURE.name}, {len(content)} bytes]")


async def test_upload_jsonl_real_fixture(client):
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    content = _JSONL_FIXTURE.read_bytes()
    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": (_JSONL_FIXTURE.name, content, "application/jsonl")},
    )
    log.check(resp.status_code == 201, f"endpoint returned 201 (got {resp.status_code})")
    result = resp.json()["data"]["results"][0]
    log.check(result["success"] is True, f"per-file success=True for {_JSONL_FIXTURE.name}")
    log.check(
        result["documents_created"] >= 1,
        f"at least one document created (got {result['documents_created']})",
    )
    log.check(result["chunks_created"] >= 1, f"at least one chunk created ({result['chunks_created']})")
    log.report(f"Upload .jsonl real fixture [{_JSONL_FIXTURE.name}, {len(content)} bytes]")


async def test_upload_multiple_files(client):
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files=[
            ("files", ("a.txt", _LONG_TEXT, "text/plain")),
            ("files", ("b.txt", _LONG_TEXT, "text/plain")),
        ],
    )
    log.check(resp.status_code == 201, "endpoint returned 201")
    results = resp.json()["data"]["results"]
    log.check(len(results) == 2, f"received per-file result for each input ({len(results)})")
    log.check(all(r["success"] for r in results), "every file reported success")
    log.report("Multi-file upload [2 files]")


async def test_upload_partial_failure_returns_per_file_results(client):
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files=[
            ("files", ("good.txt", _LONG_TEXT, "text/plain")),
            ("files", ("bad.csv", b"a,b\n1,2", "text/csv")),
        ],
    )
    log.check(resp.status_code == 201, "endpoint still returned 201 despite one failure")
    results = resp.json()["data"]["results"]
    log.check(results[0]["success"] is True, "good.txt succeeded")
    log.check(results[1]["success"] is False, "bad.csv failed")
    log.check("Unsupported" in results[1]["error"], f"error message mentions unsupported ({results[1]['error']!r})")
    log.report("Partial-failure upload [good.txt + bad.csv]")


async def test_upload_unsupported_extension(client):
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": ("data.csv", b"text\nhello", "text/csv")},
    )
    log.check(resp.status_code == 201, "endpoint returned 201 (errors are per-file)")
    result = resp.json()["data"]["results"][0]
    log.check(result["success"] is False, "csv rejected")
    log.check("Unsupported" in result["error"], f"error mentions unsupported ({result['error']!r})")
    log.report("Unsupported extension [data.csv]")


async def test_upload_oversize_file_rejected(client):
    """Upload exactly 1 byte over the 10 MB cap and verify per-file rejection."""
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    oversize = b"x" * (10 * 1024 * 1024 + 1)
    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": ("big.txt", oversize, "text/plain")},
    )
    log.check(resp.status_code == 201, "endpoint returned 201")
    result = resp.json()["data"]["results"][0]
    log.check(result["success"] is False, f"{len(oversize)}-byte file rejected")
    log.check(
        "exceeds maximum size" in result["error"],
        f"error mentions size cap ({result['error']!r})",
    )
    log.report(f"Oversize rejection [{len(oversize)} bytes > 10 MB cap]")


async def test_upload_empty_file_rejected(client):
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": ("empty.txt", b"", "text/plain")},
    )
    log.check(resp.status_code == 201, "endpoint returned 201")
    result = resp.json()["data"]["results"][0]
    log.check(result["success"] is False, "0-byte file rejected")
    log.check("empty" in result["error"], f"error mentions empty ({result['error']!r})")
    log.report("Empty-file rejection [empty.txt, 0 bytes]")


async def test_upload_normalizes_filename_to_lowercase(client):
    """Stored filenames are lowercased on input so mixed-case duplicates can't slip past dedup."""
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": ("Interview.TXT", _LONG_TEXT, "text/plain")},
    )
    log.check(resp.status_code == 201, "endpoint returned 201")
    result = resp.json()["data"]["results"][0]
    log.check(
        result["stored_filename"] == "interview.txt",
        f"'Interview.TXT' stored as '{result['stored_filename']}'",
    )
    log.report("Filename lowercased on input [Interview.TXT -> interview.txt]")


async def test_upload_duplicate_filename_case_insensitive_is_renamed(client):
    """Same filename in different casing → second upload renamed; both stored lowercase."""
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": ("interview.txt", _LONG_TEXT, "text/plain")},
    )
    resp = await client.post(
        f"{API}/corpora/{corpus_id}/upload",
        files={"files": ("Interview.TXT", _LONG_TEXT, "text/plain")},
    )
    log.check(resp.status_code == 201, "second upload returned 201")
    result = resp.json()["data"]["results"][0]
    log.check(result["success"] is True, "second upload succeeded")
    log.check(
        result["stored_filename"] == "interview (2).txt",
        f"second file renamed to '{result['stored_filename']}'",
    )

    docs_resp = await client.get(f"{API}/corpora/{corpus_id}/documents")
    filenames = sorted(d["filename"] for d in docs_resp.json()["data"]["items"])
    log.check(
        filenames == ["interview (2).txt", "interview.txt"],
        f"both filenames present in DB ({filenames})",
    )
    log.report("Case-insensitive duplicate rename [interview.txt + Interview.TXT]")


async def test_same_filename_allowed_across_different_corpora(client):
    """Uniqueness is scoped to the corpus, not global."""
    log = _AssertionLog()
    create_a = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "A"})
    create_b = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "B"})
    corpus_a = create_a.json()["data"]["id"]
    corpus_b = create_b.json()["data"]["id"]

    for label, cid in (("corpus A", corpus_a), ("corpus B", corpus_b)):
        resp = await client.post(
            f"{API}/corpora/{cid}/upload",
            files={"files": ("shared.txt", _LONG_TEXT, "text/plain")},
        )
        result = resp.json()["data"]["results"][0]
        log.check(result["success"] is True, f"{label}: upload succeeded")
        log.check(
            result["stored_filename"] == "shared.txt",
            f"{label}: stored as 'shared.txt' (no rename — different corpus)",
        )
    log.report("Cross-corpus filename reuse [shared.txt in 2 corpora]")


async def test_upload_duplicate_filename_is_renamed(client):
    """Same-case duplicate within one corpus is renamed with ' (n)' suffix."""
    log = _AssertionLog()
    create = await client.post(f"{API}/corpora", json={"project_id": P1_STR, "name": "C"})
    corpus_id = create.json()["data"]["id"]

    for _ in range(2):
        resp = await client.post(
            f"{API}/corpora/{corpus_id}/upload",
            files={"files": ("dup.txt", _LONG_TEXT, "text/plain")},
        )
        log.check(resp.status_code == 201, "upload returned 201")

    docs_resp = await client.get(f"{API}/corpora/{corpus_id}/documents")
    filenames = sorted(d["filename"] for d in docs_resp.json()["data"]["items"])
    log.check(
        filenames == ["dup (2).txt", "dup.txt"],
        f"both stored, second renamed ({filenames})",
    )
    log.report("Duplicate rename [dup.txt uploaded twice]")
