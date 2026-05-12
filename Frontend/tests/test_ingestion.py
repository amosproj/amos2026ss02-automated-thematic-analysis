"""Tests for the ingestion controllers (upload + list views).

Uses the `fake_backend` fixture from conftest.py — controllers' BackendClient
is monkey-patched so tests never hit the real network.
"""

import io


# ---------------------------------------------------------------------------
# POST /transcripts/upload
# ---------------------------------------------------------------------------


def test_upload_submit_renders_per_file_results(client, fake_backend):
    """Successful upload: backend response is rendered as a results table with
    one row per file."""
    fake_backend.upload_results = [
        {"filename": "a.txt", "stored_filename": "a.txt", "success": True,
         "documents_created": 1, "chunks_created": 2, "error": None},
        {"filename": "b.pdf", "stored_filename": "b.pdf", "success": True,
         "documents_created": 1, "chunks_created": 5, "error": None},
    ]

    resp = client.post(
        "/transcripts/upload",
        data={
            "files": [
                (io.BytesIO(b"hello world"), "a.txt"),
                (io.BytesIO(b"%PDF-1.4 fake"), "b.pdf"),
            ],
        },
        content_type="multipart/form-data",
    )

    assert resp.status_code == 200
    body = resp.data
    assert b"Upload Results" in body
    assert b"a.txt" in body
    assert b"b.pdf" in body
    assert b"Uploaded" in body
    assert b"2 succeeded, 0 failed" in body
    assert fake_backend.uploaded_files == ["a.txt", "b.pdf"]


def test_upload_submit_renders_per_file_errors(client, fake_backend):
    """Backend reports a per-file failure (e.g. unsupported extension): the
    failure row shows the backend's error message verbatim."""
    fake_backend.upload_results = [
        {"filename": "good.txt", "stored_filename": "good.txt", "success": True,
         "documents_created": 1, "chunks_created": 1, "error": None},
        {"filename": "bad.csv", "stored_filename": None, "success": False,
         "documents_created": 0, "chunks_created": 0,
         "error": "Unsupported file extension '.csv'"},
    ]

    resp = client.post(
        "/transcripts/upload",
        data={
            "files": [
                (io.BytesIO(b"ok"), "good.txt"),
                (io.BytesIO(b"a,b"), "bad.csv"),
            ],
        },
        content_type="multipart/form-data",
    )

    assert resp.status_code == 200
    assert b"1 succeeded, 1 failed" in resp.data
    assert b"Unsupported file extension" in resp.data


def test_upload_submit_surfaces_renamed_filename(client, fake_backend):
    """Duplicate filename: backend renames it; the UI mentions the renaming."""
    fake_backend.upload_results = [
        {"filename": "dup.txt", "stored_filename": "dup (2).txt", "success": True,
         "documents_created": 1, "chunks_created": 1, "error": None},
    ]

    resp = client.post(
        "/transcripts/upload",
        data={"files": [(io.BytesIO(b"x"), "dup.txt")]},
        content_type="multipart/form-data",
    )

    assert resp.status_code == 200
    assert b"Renamed" in resp.data
    assert b"dup (2).txt" in resp.data


def test_upload_submit_with_no_files_renders_form_error(client, fake_backend):
    """Submitting the form with no file selected re-renders the upload form
    with a friendly error, not a 500."""
    resp = client.post(
        "/transcripts/upload",
        data={"files": []},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    assert b"select at least one file" in resp.data


def test_upload_submit_renders_backend_error(client, fake_backend):
    """When the backend is unreachable / 5xxs, the results page shows the wrapped error."""
    fake_backend.raise_on = "upload_files"

    resp = client.post(
        "/transcripts/upload",
        data={"files": [(io.BytesIO(b"x"), "a.txt")]},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    assert b"simulated upload_files failure" in resp.data


def test_upload_rejects_oversize_file(client, fake_backend):
    """Per-file cap is enforced in the controller before forwarding."""
    huge = b"x" * (11 * 1024 * 1024)
    resp = client.post(
        "/transcripts/upload",
        data={"files": [(io.BytesIO(huge), "big.txt")]},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    assert b"at most 10 MB" in resp.data
    assert b"big.txt" in resp.data
    assert fake_backend.uploaded_files == []


# ---------------------------------------------------------------------------
# GET /transcripts/
# ---------------------------------------------------------------------------


def test_list_renders_documents_from_backend(client, fake_backend):
    fake_backend.documents = [
        {"id": "1", "title": "Interview 1", "filename": "interview1.txt",
         "created_at": "2026-05-12T10:00:00Z"},
        {"id": "2", "title": "Interview 2", "filename": "interview2.pdf",
         "created_at": "2026-05-12T11:00:00Z"},
    ]

    resp = client.get("/transcripts/")

    assert resp.status_code == 200
    assert b"interview1.txt" in resp.data
    assert b"interview2.pdf" in resp.data
    assert b"No transcripts uploaded yet" not in resp.data


def test_list_renders_empty_state(client, fake_backend):
    fake_backend.documents = []
    resp = client.get("/transcripts/")
    assert resp.status_code == 200
    assert b"No transcripts uploaded yet" in resp.data


def test_list_renders_backend_error(client, fake_backend):
    fake_backend.raise_on = "list_documents"
    resp = client.get("/transcripts/")
    assert resp.status_code == 200
    assert b"simulated list_documents failure" in resp.data
