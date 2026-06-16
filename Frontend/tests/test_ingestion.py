"""Tests for the ingestion controllers (upload + list views).

Uses the `fake_backend` fixture from conftest.py - controllers' BackendClient
is monkey-patched so tests never hit the real network.

Routes are now corpus-scoped: /transcripts/<corpus_id>/... . The FakeBackend's
ensure_corpus returns 'test-corpus-id', which is also the id we use directly
in POSTs to avoid chasing a redirect with multipart bodies.
"""

import io


CORPUS = "test-corpus-id"


# POST /transcripts/<corpus_id>/upload


def test_upload_submit_renders_per_file_results(client, fake_backend):
    """Successful upload: backend response is rendered as a results table with
    one row per file."""
    fake_backend.upload_results = [
        {"filename": "a.txt", "stored_filename": "a.txt", "success": True,
         "documents_created": 1, "error": None},
        {"filename": "b.pdf", "stored_filename": "b.pdf", "success": True,
         "documents_created": 1, "error": None},
    ]

    resp = client.post(
        f"/transcripts/{CORPUS}/upload",
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
    assert b"2 uploaded" in body
    assert b"No content" not in body
    assert fake_backend.uploaded_files == ["a.txt", "b.pdf"]


def test_upload_submit_renders_per_file_errors(client, fake_backend):
    """Backend reports a per-file failure (e.g. unsupported extension): the
    failure row shows the backend's error message verbatim."""
    fake_backend.upload_results = [
        {"filename": "good.txt", "stored_filename": "good.txt", "success": True,
         "documents_created": 1, "error": None},
        {"filename": "bad.csv", "stored_filename": None, "success": False,
         "documents_created": 0, "error": "Unsupported file extension '.csv'"},
    ]

    resp = client.post(
        f"/transcripts/{CORPUS}/upload",
        data={
            "files": [
                (io.BytesIO(b"ok"), "good.txt"),
                (io.BytesIO(b"a,b"), "bad.csv"),
            ],
        },
        content_type="multipart/form-data",
    )

    assert resp.status_code == 200
    assert b"1 uploaded" in resp.data
    assert b"1 failed" in resp.data
    assert b"Unsupported file extension" in resp.data


def test_upload_submit_surfaces_renamed_filename(client, fake_backend):
    """Duplicate filename: backend renames it; the UI mentions the renaming."""
    fake_backend.upload_results = [
        {"filename": "dup.txt", "stored_filename": "dup (2).txt", "success": True,
         "documents_created": 1, "error": None},
    ]

    resp = client.post(
        f"/transcripts/{CORPUS}/upload",
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
        f"/transcripts/{CORPUS}/upload",
        data={"files": []},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    assert b"select at least one file" in resp.data


def test_upload_submit_renders_backend_error(client, fake_backend):
    """When the backend is unreachable / 5xxs, the results page shows the wrapped error."""
    fake_backend.raise_on = "upload_files"

    resp = client.post(
        f"/transcripts/{CORPUS}/upload",
        data={"files": [(io.BytesIO(b"x"), "a.txt")]},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    assert b"simulated upload_files failure" in resp.data
    assert b"Traceback" not in resp.data


def test_upload_submit_shows_unavailable_message_when_backend_down(client, fake_backend):
    from web.services.backend_client import BackendUnavailableError

    fake_backend.raise_on = ("upload_files", BackendUnavailableError)
    resp = client.post(
        f"/transcripts/{CORPUS}/upload",
        data={"files": [(io.BytesIO(b"x"), "a.txt")]},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    # Substring chosen to avoid the apostrophe in "can't" (Jinja2 HTML-escapes it).
    assert b"reach the analysis service" in resp.data


def test_upload_submit_re_renders_form_on_validation_error(client, fake_backend):
    """A BackendValidationError should send the user back to the form (not the
    results page) so they can fix the input."""
    from web.services.backend_client import BackendValidationError

    fake_backend.raise_on = ("upload_files", BackendValidationError)
    resp = client.post(
        f"/transcripts/{CORPUS}/upload",
        data={"files": [(io.BytesIO(b"x"), "a.txt")]},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    # Upload form markers — confirms we re-rendered the form, not the results page.
    assert b"Select files" in resp.data
    assert b"Upload Results" not in resp.data


def test_upload_form_shows_corpus_selector_for_both_upload_forms(client, fake_backend):
    fake_backend.corpora = [
        {"id": CORPUS, "name": "Main Corpus"},
        {"id": "second-corpus-id", "name": "Pilot Corpus"},
    ]

    resp = client.get(f"/transcripts/{CORPUS}/upload")

    assert resp.status_code == 200
    assert b'id="global-corpus-select"' in resp.data
    assert b"Main Corpus" in resp.data
    assert b"Pilot Corpus" in resp.data
    assert b"data-switch-template" in resp.data
    assert b"/transcripts/test-corpus-id/upload" in resp.data
    assert b"/demographic/test-corpus-id/upload" in resp.data
    assert b'id="create-corpus-form"' in resp.data
    assert b"/transcripts/corpora" in resp.data


def test_create_corpus_from_upload_page_redirects_to_new_corpus(client, fake_backend):
    fake_backend.corpora = [{"id": CORPUS, "name": "Main Corpus"}]

    resp = client.post(
        "/transcripts/corpora",
        data={"name": "Pilot Corpus", "current_corpus_id": CORPUS},
        follow_redirects=False,
    )

    assert resp.status_code == 302
    assert fake_backend.last_created_corpus is not None
    new_id = fake_backend.last_created_corpus["id"]
    assert resp.headers["Location"].endswith(f"/transcripts/{new_id}/upload")

    follow = client.get(resp.headers["Location"])
    assert follow.status_code == 200
    assert b"Pilot Corpus" in follow.data


def test_create_corpus_requires_name_and_stays_on_current_upload(client, fake_backend):
    resp = client.post(
        "/transcripts/corpora",
        data={"name": "   ", "current_corpus_id": CORPUS},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith(f"/transcripts/{CORPUS}/upload")


def test_upload_rejects_oversize_file(client, fake_backend):
    """Per-file cap is enforced in the controller before forwarding."""
    huge = b"x" * (11 * 1024 * 1024)
    resp = client.post(
        f"/transcripts/{CORPUS}/upload",
        data={"files": [(io.BytesIO(huge), "big.txt")]},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 200
    assert b"at most 10 MB" in resp.data
    assert b"big.txt" in resp.data
    assert fake_backend.uploaded_files == []


# GET /transcripts/  (landing -> 302 -> /transcripts/<corpus_id>/)


def test_list_renders_documents_from_backend(client, fake_backend):
    fake_backend.documents = [
        {"id": "1", "title": "Interview 1", "filename": "interview1.txt",
         "created_at": "2026-05-12T10:00:00Z"},
        {"id": "2", "title": "Interview 2", "filename": "interview2.pdf",
         "created_at": "2026-05-12T11:00:00Z"},
    ]

    resp = client.get("/transcripts/", follow_redirects=True)

    assert resp.status_code == 200
    assert b"interview1.txt" in resp.data
    assert b"interview2.pdf" in resp.data
    assert b"data-selectable-list" in resp.data
    assert b"data-selectable-list-select-all" in resp.data
    assert resp.data.count(b"data-selectable-list-checkbox") == 2
    assert b"0 transcripts selected" in resp.data
    assert b"Delete selected" in resp.data
    assert b'id="deleteSelectedTranscriptsModal"' in resp.data
    assert b"Yes, Delete Transcripts" in resp.data
    assert b"<th>Filename</th>" in resp.data
    assert b'<th class="text-end">Actions</th>' in resp.data
    assert b"deleteTranscriptModal-" not in resp.data
    assert b"Yes, Delete Transcript</button>" not in resp.data
    assert b"No transcripts uploaded yet" not in resp.data


def test_list_renders_empty_state(client, fake_backend):
    fake_backend.documents = []
    resp = client.get("/transcripts/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"No transcripts uploaded yet" in resp.data


def test_list_renders_backend_error(client, fake_backend):
    fake_backend.raise_on = "list_documents"
    resp = client.get("/transcripts/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"simulated list_documents failure" in resp.data
    assert b"Traceback" not in resp.data


def test_list_shows_unavailable_message_when_backend_down(client, fake_backend):
    from web.services.backend_client import BackendUnavailableError

    fake_backend.raise_on = ("list_documents", BackendUnavailableError)
    resp = client.get("/transcripts/", follow_redirects=True)
    assert resp.status_code == 200
    # Substring chosen to avoid the apostrophe in "can't" (Jinja2 HTML-escapes it).
    assert b"reach the analysis service" in resp.data
    assert b"No transcripts uploaded yet" not in resp.data


# POST /transcripts/<corpus_id>/<document_id>/delete


def test_delete_transcript_success(client, fake_backend):
    fake_backend.documents = [
        {"id": "doc-1", "title": "Interview 1", "filename": "1.txt", "created_at": "2026-05-12"},
        {"id": "doc-2", "title": "Interview 2", "filename": "2.txt", "created_at": "2026-05-13"}
    ]

    resp = client.post(f"/transcripts/{CORPUS}/doc-1/delete")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith(f"/transcripts/{CORPUS}/")
    
    # Check flash message
    follow = client.get(resp.headers["Location"])
    assert b"Transcript deleted successfully" in follow.data
    assert len(fake_backend.documents) == 1
    assert fake_backend.documents[0]["id"] == "doc-2"


def test_delete_transcript_backend_error(client, fake_backend):
    fake_backend.raise_on = "delete_document"
    resp = client.post(f"/transcripts/{CORPUS}/doc-1/delete", follow_redirects=True)
    assert resp.status_code == 200
    assert b"simulated delete_document failure" in resp.data


# POST /transcripts/<corpus_id>/delete


def test_delete_corpus_success(client, fake_backend):
    fake_backend.corpora = [
        {"id": CORPUS, "name": "Corpus 1"},
        {"id": "other-id", "name": "Corpus 2"}
    ]
    
    resp = client.post(f"/transcripts/{CORPUS}/delete")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/transcripts/")

    follow = client.get(resp.headers["Location"], follow_redirects=True)
    assert b"Corpus deleted successfully" in follow.data
    assert len(fake_backend.corpora) == 1
    assert fake_backend.corpora[0]["id"] == "other-id"


def test_delete_corpus_backend_error(client, fake_backend):
    fake_backend.raise_on = "delete_corpus"
    resp = client.post(f"/transcripts/{CORPUS}/delete", follow_redirects=True)
    assert resp.status_code == 200
    assert b"simulated delete_corpus failure" in resp.data

def test_delete_selected_transcripts_success(client, fake_backend):
    fake_backend.documents = [
        {"id": "doc-1", "title": "Interview 1", "filename": "1.txt", "created_at": "2026-05-12"},
        {"id": "doc-2", "title": "Interview 2", "filename": "2.txt", "created_at": "2026-05-13"},
        {"id": "doc-3", "title": "Interview 3", "filename": "3.txt", "created_at": "2026-05-14"},
    ]

    resp = client.post(
        f"/transcripts/{CORPUS}/delete_transcripts",
        data={"item_ids": ["doc-1", "doc-2"]},
        follow_redirects=True,
    )

    assert resp.status_code == 200
    assert b"Deleted 2 transcripts" in resp.data
    assert [d["id"] for d in fake_backend.documents] == ["doc-3"]


def test_delete_selected_transcripts_requires_selection(client, fake_backend):
    resp = client.post(f"/transcripts/{CORPUS}/delete_transcripts", data={}, follow_redirects=True)
    assert resp.status_code == 200
    assert b"Select at least one transcript to delete" in resp.data
