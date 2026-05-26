"""Tests for the demographic data controllers (list, upload, preview, view).

Uses the `fake_backend` fixture from conftest.py - controllers' BackendClient
is monkey-patched so tests never hit the real network.

Routes are corpus-scoped: /demographic/<corpus_id>/... . The FakeBackend's
ensure_corpus returns 'test-corpus-id'.
"""

import io


CORPUS = "test-corpus-id"


# ---- List page --------------------------------------------------------------


def test_list_renders_demographic_files(client, fake_backend):
    """Files are rendered in the table with correct metadata."""
    fake_backend.demographic_files = [
        {
            "id": "file-1",
            "corpus_id": CORPUS,
            "name": "participants",
            "original_columns": ["username", "age", "gender"],
            "rows_total": 5,
            "created_at": "2026-05-20T10:00:00Z",
            "updated_at": "2026-05-20T10:00:00Z",
        },
    ]

    resp = client.get("/demographic/", follow_redirects=True)

    assert resp.status_code == 200
    assert b"participants" in resp.data
    assert b"View Data" in resp.data
    assert b"No demographic data uploaded yet" not in resp.data


def test_list_renders_empty_state(client, fake_backend):
    fake_backend.demographic_files = []
    resp = client.get("/demographic/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"No demographic data uploaded yet" in resp.data


def test_list_renders_backend_error(client, fake_backend):
    fake_backend.raise_on = "list_demographic_files"
    resp = client.get("/demographic/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"simulated list_demographic_files failure" in resp.data
    assert b"Traceback" not in resp.data


def test_list_landing_redirects(client, fake_backend):
    resp = client.get("/demographic/")
    assert resp.status_code == 302
    assert f"/demographic/{CORPUS}/" in resp.headers["Location"]


# ---- Upload form ------------------------------------------------------------


def test_upload_form_renders(client, fake_backend):
    resp = client.get(f"/demographic/{CORPUS}/upload")
    assert resp.status_code == 200
    assert b"Upload Demographic Data" in resp.data
    assert b"Select CSV file" in resp.data


def test_upload_submit_no_file_shows_error(client, fake_backend):
    resp = client.post(
        f"/demographic/{CORPUS}/upload",
        data={"file": (io.BytesIO(b""), "")},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    assert b"select a CSV file" in resp.data


def test_upload_submit_redirects_to_preview(client, fake_backend):
    fake_backend.demographic_upload_response = {
        "import_id": "import-abc",
        "name": "test_data",
        "status": "pending",
        "preview": {
            "rows_detected": 3,
            "columns_detected": 3,
            "sample_rows": [
                {"username": "alice", "age": "30", "gender": "F"},
                {"username": "bob", "age": "25", "gender": "M"},
            ],
        },
        "expires_at": "2026-05-20T11:00:00Z",
    }

    resp = client.post(
        f"/demographic/{CORPUS}/upload",
        data={"file": (io.BytesIO(b"username;age;gender\nalice;30;F\n"), "test.csv")},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302
    assert "/preview/import-abc" in resp.headers["Location"]


def test_upload_submit_backend_error(client, fake_backend):
    fake_backend.raise_on = "upload_demographic"
    resp = client.post(
        f"/demographic/{CORPUS}/upload",
        data={"file": (io.BytesIO(b"username;age\nalice;30\n"), "test.csv")},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    assert b"simulated upload_demographic failure" in resp.data


# ---- Preview page -----------------------------------------------------------


def _seed_preview_session(client, fake_backend):
    """Upload a CSV so preview data is stored in the session."""
    fake_backend.demographic_upload_response = {
        "import_id": "import-xyz",
        "name": "demo_data",
        "status": "pending",
        "preview": {
            "rows_detected": 2,
            "columns_detected": 3,
            "sample_rows": [
                {"username": "alice", "age": "30", "gender": "F"},
                {"username": "bob", "age": "25", "gender": "M"},
            ],
        },
        "expires_at": "2026-05-20T11:00:00Z",
    }
    client.post(
        f"/demographic/{CORPUS}/upload",
        data={"file": (io.BytesIO(b"username;age;gender\nalice;30;F\n"), "demo.csv")},
        content_type="multipart/form-data",
    )


def test_preview_renders_sample_rows(client, fake_backend):
    _seed_preview_session(client, fake_backend)
    resp = client.get(f"/demographic/{CORPUS}/preview/import-xyz")
    assert resp.status_code == 200
    assert b"Preview" in resp.data
    assert b"alice" in resp.data
    assert b"bob" in resp.data
    assert b"Confirm Upload" in resp.data
    assert b"Discard" in resp.data


def test_preview_expired_redirects_to_upload(client, fake_backend):
    """If preview data isn't in the session, redirect back to upload."""
    resp = client.get(
        f"/demographic/{CORPUS}/preview/nonexistent-id",
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert "/upload" in resp.headers["Location"]


def test_preview_confirm_redirects_to_list(client, fake_backend):
    _seed_preview_session(client, fake_backend)
    fake_backend.demographic_confirm_response = {
        "import_id": "import-xyz",
        "name": "demo_data",
        "rows_created": 2,
        "status": "Demographic data successfully uploaded",
    }
    resp = client.post(
        f"/demographic/{CORPUS}/preview/import-xyz",
        data={"action": "confirm"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"uploaded successfully" in resp.data


def test_preview_discard_redirects_to_list(client, fake_backend):
    _seed_preview_session(client, fake_backend)
    fake_backend.demographic_confirm_response = {
        "import_id": "import-xyz",
        "name": "demo_data",
        "rows_created": 0,
        "status": "Upload cancelled by user",
    }
    resp = client.post(
        f"/demographic/{CORPUS}/preview/import-xyz",
        data={"action": "discard"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert b"Upload discarded" in resp.data


# ---- View page --------------------------------------------------------------


def test_view_renders_data_table_with_columns(client, fake_backend):
    fake_backend.demographic_files = [
        {
            "id": "file-1",
            "corpus_id": CORPUS,
            "name": "participants",
            "original_columns": ["username", "age", "gender"],
            "rows_total": 2,
            "created_at": "2026-05-20T10:00:00Z",
            "updated_at": "2026-05-20T10:00:00Z",
        },
    ]
    fake_backend.demographic_rows = [
        {
            "id": "row-1",
            "demographic_file_id": "file-1",
            "interviewee_id": "alice",
            "row_number": 1,
            "data": {"age": "30", "gender": "F"},
        },
        {
            "id": "row-2",
            "demographic_file_id": "file-1",
            "interviewee_id": "bob",
            "row_number": 2,
            "data": {"age": "25", "gender": "M"},
        },
    ]

    resp = client.get(f"/demographic/{CORPUS}/view/file-1")
    assert resp.status_code == 200
    assert b"participants" in resp.data
    assert b"alice" in resp.data
    assert b"bob" in resp.data
    assert b"age" in resp.data
    assert b"gender" in resp.data


def test_view_shows_linked_transcript(client, fake_backend):
    fake_backend.demographic_files = [
        {
            "id": "file-1",
            "corpus_id": CORPUS,
            "name": "participants",
            "original_columns": ["username", "age"],
            "rows_total": 1,
            "created_at": "2026-05-20T10:00:00Z",
            "updated_at": "2026-05-20T10:00:00Z",
        },
    ]
    fake_backend.demographic_rows = [
        {
            "id": "row-1",
            "demographic_file_id": "file-1",
            "interviewee_id": "alice",
            "row_number": 1,
            "data": {"age": "30"},
        },
    ]
    fake_backend.demographic_link_summary = {
        "total_transcripts": 1,
        "matched": 1,
        "details": [
            {
                "document_id": "doc-1",
                "document_title": "Alice Interview",
                "demographic_row_id": "row-1",
                "matched": True,
            },
        ],
    }

    resp = client.get(f"/demographic/{CORPUS}/view/file-1")
    assert resp.status_code == 200
    assert b"Alice Interview" in resp.data


def test_view_shows_unlinked_indicator(client, fake_backend):
    fake_backend.demographic_files = [
        {
            "id": "file-1",
            "corpus_id": CORPUS,
            "name": "participants",
            "original_columns": ["username", "age"],
            "rows_total": 1,
            "created_at": "2026-05-20T10:00:00Z",
            "updated_at": "2026-05-20T10:00:00Z",
        },
    ]
    fake_backend.demographic_rows = [
        {
            "id": "row-1",
            "demographic_file_id": "file-1",
            "interviewee_id": "alice",
            "row_number": 1,
            "data": {"age": "30"},
        },
    ]
    fake_backend.demographic_link_summary = {
        "total_transcripts": 0,
        "matched": 0,
        "details": [],
    }

    resp = client.get(f"/demographic/{CORPUS}/view/file-1")
    assert resp.status_code == 200
    assert b"Not linked" in resp.data


def test_view_renders_backend_error(client, fake_backend):
    fake_backend.raise_on = "list_demographic_files"
    resp = client.get(f"/demographic/{CORPUS}/view/file-1")
    assert resp.status_code == 200
    assert b"simulated list_demographic_files failure" in resp.data
    assert b"Traceback" not in resp.data


def test_view_renders_empty_when_no_rows(client, fake_backend):
    fake_backend.demographic_files = [
        {
            "id": "file-1",
            "corpus_id": CORPUS,
            "name": "empty_file",
            "original_columns": ["username", "age"],
            "rows_total": 0,
            "created_at": "2026-05-20T10:00:00Z",
            "updated_at": "2026-05-20T10:00:00Z",
        },
    ]
    fake_backend.demographic_rows = []

    resp = client.get(f"/demographic/{CORPUS}/view/file-1")
    assert resp.status_code == 200
    assert b"No data rows" in resp.data
