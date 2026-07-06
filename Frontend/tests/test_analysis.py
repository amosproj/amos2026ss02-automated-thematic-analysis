import re


def test_analysis_index_no_corpus(client, fake_backend):
    # Setup state to simulate backend failure which results in no active corpus
    fake_backend.raise_on = "list_corpora"
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = None
        
    resp = client.get("/analysis/")
    assert resp.status_code == 200
    assert b"No active corpus selected." in resp.data
    assert b"disabled style=\"pointer-events: none;\"" in resp.data

def test_analysis_index_no_transcripts(client, fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [] # No documents
    fake_backend.codebooks = [{"id": "cb1", "name": "Test CB", "version": 1}]
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id
        
    resp = client.get("/analysis/")
    assert resp.status_code == 200
    assert b"Corpus has no transcripts." in resp.data
    assert b"disabled style=\"pointer-events: none;\"" in resp.data

def test_analysis_index_no_codebook(client, fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [{"id": "doc1"}]
    fake_backend.codebooks = [] # No codebook
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id
        
    resp = client.get("/analysis/")
    assert resp.status_code == 200
    assert b"Please configure a codebook for this corpus." in resp.data
    assert b"disabled style=\"pointer-events: none;\"" in resp.data

def test_analysis_index_ready(client, fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [{"id": "doc1"}]
    fake_backend.codebooks = [{"id": "cb1", "name": "Test CB", "version": 1}]
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id
        
    resp = client.get("/analysis/")
    assert resp.status_code == 200
    # Button should NOT be disabled
    assert b"disabled style=\"pointer-events: none;\"" not in resp.data

def test_trigger_analysis_success(client, fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [{"id": "doc1"}]
    fake_backend.codebooks = [{"id": "cb1", "name": "Test CB", "version": 1}]
    
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id
        
    resp = client.post("/analysis/trigger", data={
        "corpus_id": corpus_id,
        "codebook_id": "cb1",
        "name": "Test Run",
        "custom_id": "custom-123",
        "transcript_document_ids": ["doc1"]
    })
    
    assert resp.status_code == 302
    assert "/analysis/job/" in resp.headers["Location"]

def test_trigger_analysis_missing_data(client, fake_backend):
    resp = client.post("/analysis/trigger", data={
        "corpus_id": "test-corpus",
        # missing codebook_id
    })
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/analysis/")
    
    follow = client.get(resp.headers["Location"])
    assert b"Missing corpus or codebook ID." in follow.data

def test_analysis_wait_page(client):
    resp = client.get("/analysis/job/test-job")
    assert resp.status_code == 200
    assert b"Applying Codebook" in resp.data
    assert b'data-cancel-url="/analysis/job/test-job/cancel"' in resp.data
    assert b"/api/v1/codebooks/apply-jobs/" not in resp.data

def test_analysis_job_status(client, fake_backend):
    job = fake_backend.trigger_analysis("test", "cb1")
    resp = client.get(f"/analysis/job/{job['id']}/status")
    assert resp.status_code == 200
    data = resp.json
    assert data["id"] == job["id"]
    assert data["status"] == "queued"


def test_analysis_job_cancel(client, fake_backend):
    resp = client.post("/analysis/job/job-123/cancel")
    assert resp.status_code == 200
    assert fake_backend.cancelled_analysis_job_ids == ["job-123"]
    assert resp.json["cancel_requested"] is True


# Delete Analysis Runs (issue #203) ------------------------------------------


def _ready_corpus_with_runs(fake_backend):
    corpus_id = "test-corpus"
    fake_backend.corpora = [{"id": corpus_id, "name": "Test Corpus"}]
    fake_backend.documents = [{"id": "doc1"}]
    fake_backend.codebooks = [{"id": "cb1", "name": "Test CB", "version": 1}]
    fake_backend.application_runs = [
        {"id": "run1", "codebook_id": "cb1", "name": "Initial Run",
         "custom_id": "RUN-001", "status": "succeeded",
         "created_at": "2026-01-01T00:00:00", "transcript_document_ids": ["doc1"]},
        {"id": "run2", "codebook_id": "cb1", "name": "Second Run",
         "custom_id": "RUN-002", "status": "failed",
         "created_at": "2026-01-02T00:00:00", "transcript_document_ids": ["doc1"]},
    ]
    return corpus_id


def test_previous_runs_render_with_delete_toolbar(client, fake_backend):
    corpus_id = _ready_corpus_with_runs(fake_backend)
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id

    resp = client.get("/analysis/")
    assert resp.status_code == 200
    body = resp.data
    # Selectable-list delete affordances are present.
    assert b"Delete selected" in body
    assert b"data-selectable-list" in body
    # Each run is a selectable row.
    assert b'data-item-id="run1"' in body
    assert b'value="run1"' in body
    assert b"Initial Run" in body


def test_previous_runs_render_view_analysis_link(client, fake_backend):
    corpus_id = _ready_corpus_with_runs(fake_backend)
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id

    resp = client.get("/analysis/")
    assert resp.status_code == 200
    body = resp.data
    assert b"<th>Actions</th>" in body
    assert b"View Analysis" in body
    assert (
        b'href="/codebooks/test-corpus/cb1/themes?application_run_id=run1"'
        in body
    )


def test_delete_selected_runs_success(client, fake_backend):
    corpus_id = _ready_corpus_with_runs(fake_backend)
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id

    resp = client.post(
        f"/analysis/runs/delete?corpus_id={corpus_id}",
        data={"item_ids": ["run1", "run2"]},
    )
    assert resp.status_code == 302
    assert "/analysis/" in resp.headers["Location"]
    assert fake_backend.deleted_run_ids == ["run1", "run2"]

    follow = client.get(resp.headers["Location"])
    assert b"Deleted 2 analysis runs." in follow.data


def test_delete_selected_runs_none_selected(client, fake_backend):
    corpus_id = _ready_corpus_with_runs(fake_backend)
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id

    resp = client.post(f"/analysis/runs/delete?corpus_id={corpus_id}", data={})
    assert resp.status_code == 302
    assert fake_backend.deleted_run_ids == []

    follow = client.get(resp.headers["Location"])
    assert b"Select at least one analysis run to delete." in follow.data


def test_delete_selected_runs_backend_error_is_flashed(client, fake_backend):
    corpus_id = _ready_corpus_with_runs(fake_backend)
    fake_backend.raise_on = "delete_codebook_application_run"
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id

    resp = client.post(
        f"/analysis/runs/delete?corpus_id={corpus_id}",
        data={"item_ids": ["run1"]},
    )
    assert resp.status_code == 302
    follow = client.get(resp.headers["Location"])
    assert b"simulated delete_codebook_application_run failure" in follow.data


def test_previous_runs_transcript_count_and_popup_trigger(client, fake_backend):
    corpus_id = _ready_corpus_with_runs(fake_backend)
    fake_backend.documents = [
        {"id": "doc1", "title": "Interview Alpha"},
        {"id": "doc2", "title": "Interview Beta"},
    ]
    fake_backend.application_runs[0]["transcript_document_ids"] = ["doc1", "doc2"]
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id

    resp = client.get("/analysis/")
    assert resp.status_code == 200
    body = resp.data
    # The old hard-to-scroll dropdown is gone entirely.
    assert b"dropdown" not in body
    # Transcripts column shows a bare integer count.
    assert re.search(rb'<td class="text-secondary">\s*2\s*</td>', body)
    # Succeeded run gets a View Transcripts trigger wired to its run id...
    assert b"View Transcripts" in body
    assert b'data-view-transcripts' in body
    assert b'data-run-id="run1"' in body
    # ...but the failed run does not.
    assert b'aria-label="View transcripts used in analysis run Second Run"' not in body
    # Shared modal skeleton + read-URL template are on the page.
    assert b'id="runTranscriptsModal"' in body
    assert b'id="runTranscriptsSearch"' in body
    assert b"__DOC__" in body and b"__RUN__" in body
    # Resolved titles are serialized into previousRuns for the modal JS.
    assert b"transcript_docs" in body
    assert b"Interview Alpha" in body
    assert b"Interview Beta" in body


def test_previous_runs_transcript_title_fallback_for_unknown_doc(client, fake_backend):
    corpus_id = _ready_corpus_with_runs(fake_backend)
    fake_backend.documents = [{"id": "other-doc", "title": "Interview Alpha"}]
    fake_backend.application_runs[0]["transcript_document_ids"] = ["doc-gone-12345"]
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id

    resp = client.get("/analysis/")
    assert resp.status_code == 200
    body = resp.data
    # Unknown ids fall back to a shortened id (tojson may escape the ellipsis).
    assert (b"doc-gone\\u2026" in body) or ("doc-gone…".encode() in body)


def test_previous_runs_without_transcripts_show_dash(client, fake_backend):
    corpus_id = _ready_corpus_with_runs(fake_backend)
    fake_backend.application_runs = [
        {"id": "run3", "codebook_id": "cb1", "name": "Running Run",
         "custom_id": "RUN-003", "status": "running",
         "created_at": "2026-01-03T00:00:00", "transcript_document_ids": []},
    ]
    with client.session_transaction() as sess:
        sess["active_corpus_id"] = corpus_id

    resp = client.get("/analysis/")
    assert resp.status_code == 200
    body = resp.data
    # No trigger button is rendered (data-run-id is unique to it); the shared
    # modal skeleton and its JS may still be present on the page.
    assert b"data-run-id=" not in body
    assert re.search(rb'<td class="text-secondary">\s*<span class="text-muted small">', body)
