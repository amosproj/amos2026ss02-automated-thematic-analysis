"""Tests for the transcript read view (ingestion.read_transcript).

Covers:
- Basic render without a run selected (analysis panel shows available runs,
  no highlight JS is injected).
- Render with a run_id and matching code assignments (highlight script block
  and toggle are injected; themes and quotes appear in the page data).
- Render with a run_id but no document coding found for this document
  (graceful "no quotes" message, no highlight script).
- Backend error when fetching run documents (highlights degrade gracefully,
  transcript still renders).
- Only quotes with an accepted quote_match_status are forwarded to the template.
- Unit test for the _flatten_theme_tree helper.
"""


CORPUS = "test-corpus-id"
DOCUMENT_ID = "doc-1"
RUN_ID = "run-abc"
CODEBOOK_ID = "cb-1"
THEME_ID = "theme-1"

READ_URL = f"/transcripts/{CORPUS}/{DOCUMENT_ID}/read"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_run(run_id=RUN_ID, codebook_id=CODEBOOK_ID, status="succeeded"):
    return {
        "id": run_id,
        "codebook_id": codebook_id,
        "name": "Test Run",
        "custom_id": "RUN-001",
        "status": status,
        "created_at": "2026-06-01T10:00:00Z",
        "transcript_document_ids": [DOCUMENT_ID],
    }


def _make_code_assignment(quote="Hello world", status="exact", theme_id=THEME_ID):
    return {
        "id": "ca-1",
        "code_id": "code-1",
        "theme_id": theme_id,
        "quote": quote,
        "start_char": 0,
        "end_char": len(quote),
        "quote_match_status": status,
        "confidence": 0.9,
        "rationale": None,
    }


def _make_document_coding(document_id=DOCUMENT_ID, code_assignments=None):
    return {
        "id": "dc-1",
        "application_run_id": RUN_ID,
        "document_id": document_id,
        "codebook_id": CODEBOOK_ID,
        "status": "coded",
        "theme_assignments": [],
        "code_assignments": code_assignments or [],
    }


def _make_theme_tree():
    return [
        {
            "theme": {
                "id": THEME_ID,
                "label": "Communication",
                "description": "All about how people communicate.",
                "is_active": True,
                "node_type": "THEME",
            },
            "children": [],
        }
    ]


# ── Basic render ───────────────────────────────────────────────────────────────

def test_read_transcript_renders(client, fake_backend):
    """Transcript title and content appear; analysis panel is present."""
    resp = client.get(READ_URL)

    assert resp.status_code == 200
    assert b"Interview 1" in resp.data
    assert b"Analysis Highlights" in resp.data


def test_read_transcript_no_run_shows_available_runs(client, fake_backend):
    """With no run_id, the dropdown lists available succeeded runs."""
    fake_backend.codebooks = [{"id": CODEBOOK_ID, "name": "My Codebook", "corpus_id": CORPUS, "version": 1}]
    fake_backend.application_runs = [_make_run()]

    resp = client.get(READ_URL)

    assert resp.status_code == 200
    assert b"My Codebook" in resp.data
    assert b"Test Run" in resp.data
    # No highlight JS injected when no run is selected
    assert b"CODE_ASSIGNMENTS" not in resp.data


def test_read_transcript_no_runs_shows_empty_state(client, fake_backend):
    """When no runs exist the panel shows the empty-state message."""
    fake_backend.codebooks = []

    resp = client.get(READ_URL)

    assert resp.status_code == 200
    assert b"No completed analysis runs available" in resp.data


# ── With run selected ─────────────────────────────────────────────────────────

def test_read_transcript_with_run_injects_highlight_script(client, fake_backend):
    """Selecting a run with quotes injects the highlight JS block."""
    fake_backend.codebooks = [{"id": CODEBOOK_ID, "name": "My Codebook", "corpus_id": CORPUS, "version": 1}]
    fake_backend.application_runs = [_make_run()]
    fake_backend.run_documents[RUN_ID] = [
        _make_document_coding(code_assignments=[_make_code_assignment()])
    ]
    fake_backend.theme_tree = _make_theme_tree()

    resp = client.get(f"{READ_URL}?run_id={RUN_ID}")

    assert resp.status_code == 200
    assert b"CODE_ASSIGNMENTS" in resp.data
    assert b"Hello world" in resp.data
    assert b"Communication" in resp.data
    assert b"toggleHighlights" in resp.data


def test_read_transcript_with_run_shows_quote_count_badge(client, fake_backend):
    """The quote count badge reflects the number of accepted assignments."""
    fake_backend.codebooks = [{"id": CODEBOOK_ID, "name": "CB", "corpus_id": CORPUS, "version": 1}]
    fake_backend.application_runs = [_make_run()]
    fake_backend.run_documents[RUN_ID] = [
        _make_document_coding(code_assignments=[
            _make_code_assignment("Quote one", "exact"),
            _make_code_assignment("Quote two", "normalized"),
        ])
    ]
    fake_backend.theme_tree = _make_theme_tree()

    resp = client.get(f"{READ_URL}?run_id={RUN_ID}")

    assert resp.status_code == 200
    assert b"2 quotes" in resp.data


def test_read_transcript_run_selected_is_preselected_in_dropdown(client, fake_backend):
    """The run passed via ?run_id= is marked selected in the <select>."""
    fake_backend.codebooks = [{"id": CODEBOOK_ID, "name": "CB", "corpus_id": CORPUS, "version": 1}]
    fake_backend.application_runs = [_make_run()]
    fake_backend.run_documents[RUN_ID] = [_make_document_coding()]
    fake_backend.theme_tree = []

    resp = client.get(f"{READ_URL}?run_id={RUN_ID}")

    assert resp.status_code == 200
    # The option for this run must carry the selected attribute
    assert f'value="{RUN_ID}"'.encode() in resp.data
    assert b"selected" in resp.data


# ── No document coding found ───────────────────────────────────────────────────

def test_read_transcript_no_coding_for_document_shows_message(client, fake_backend):
    """If the run has no coding for this document, show the 'no quotes' message."""
    fake_backend.codebooks = [{"id": CODEBOOK_ID, "name": "CB", "corpus_id": CORPUS, "version": 1}]
    fake_backend.application_runs = [_make_run()]
    # Run documents exist but for a different document
    fake_backend.run_documents[RUN_ID] = [
        _make_document_coding(document_id="other-doc-id")
    ]
    fake_backend.theme_tree = []

    resp = client.get(f"{READ_URL}?run_id={RUN_ID}")

    assert resp.status_code == 200
    assert b"No quotes found" in resp.data
    assert b"CODE_ASSIGNMENTS" not in resp.data


# ── quote_match_status filtering ──────────────────────────────────────────────

def test_read_transcript_filters_not_found_quotes(client, fake_backend):
    """Quotes with status 'not_found' must not be forwarded to the JS block."""
    fake_backend.codebooks = [{"id": CODEBOOK_ID, "name": "CB", "corpus_id": CORPUS, "version": 1}]
    fake_backend.application_runs = [_make_run()]
    fake_backend.run_documents[RUN_ID] = [
        _make_document_coding(code_assignments=[
            _make_code_assignment("Good quote", "exact"),
            _make_code_assignment("Missing quote", "not_found"),
        ])
    ]
    fake_backend.theme_tree = _make_theme_tree()

    resp = client.get(f"{READ_URL}?run_id={RUN_ID}")

    assert resp.status_code == 200
    assert b"Good quote" in resp.data
    assert b"Missing quote" not in resp.data
    # Only 1 quote passed the filter
    assert b"1 quote" in resp.data


# ── Graceful degradation ───────────────────────────────────────────────────────

def test_read_transcript_backend_error_on_run_documents_still_renders(client, fake_backend):
    """A backend error fetching run documents degrades gracefully."""
    fake_backend.codebooks = [{"id": CODEBOOK_ID, "name": "CB", "corpus_id": CORPUS, "version": 1}]
    fake_backend.application_runs = [_make_run()]
    fake_backend.raise_on = "get_codebook_application_run_documents"

    resp = client.get(f"{READ_URL}?run_id={RUN_ID}")

    assert resp.status_code == 200
    assert b"Interview 1" in resp.data
    assert b"CODE_ASSIGNMENTS" not in resp.data


def test_read_transcript_backend_error_on_runs_list_still_renders(client, fake_backend):
    """A backend error fetching the runs list still shows the transcript."""
    fake_backend.raise_on = "list_codebooks"

    resp = client.get(READ_URL)

    assert resp.status_code == 200
    assert b"Interview 1" in resp.data


def test_read_transcript_document_not_found_redirects(client, fake_backend):
    """A backend error on get_document_content redirects to the transcript list."""
    fake_backend.raise_on = "get_document_content"

    resp = client.get(READ_URL)

    assert resp.status_code == 302
    assert "/transcripts/" in resp.headers["Location"]


# ── Unit test: _flatten_theme_tree ────────────────────────────────────────────

def test_flatten_theme_tree_flat():
    from web.controllers.ingestion import _flatten_theme_tree

    tree = [
        {"theme": {"id": "t1", "label": "Theme A", "description": "Desc A"}, "children": []},
        {"theme": {"id": "t2", "label": "Theme B", "description": None}, "children": []},
    ]
    result = _flatten_theme_tree(tree)

    assert result == {
        "t1": {"label": "Theme A", "description": "Desc A"},
        "t2": {"label": "Theme B", "description": ""},
    }


def test_flatten_theme_tree_nested():
    from web.controllers.ingestion import _flatten_theme_tree

    tree = [
        {
            "theme": {"id": "parent", "label": "Parent", "description": "P"},
            "children": [
                {"theme": {"id": "child", "label": "Child", "description": "C"}, "children": []},
            ],
        }
    ]
    result = _flatten_theme_tree(tree)

    assert "parent" in result
    assert "child" in result
    assert result["child"]["label"] == "Child"


def test_flatten_theme_tree_empty():
    from web.controllers.ingestion import _flatten_theme_tree

    assert _flatten_theme_tree([]) == {}
