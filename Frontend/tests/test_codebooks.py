# test_codebooks.py — tests for the codebooks blueprint (web/controllers/codebooks.py).
#
# Covers two routes:
#   GET /codebooks/              — list all codebooks
#   GET /codebooks/<id>/themes   — theme browser for a specific codebook
#
# All tests use the `client` and `fake_backend` fixtures from conftest.py.
# The fake_backend replaces the real BackendClient, so no running backend is needed.
# Tests assert on the raw HTML bytes returned by the Flask test client (resp.data).
#
# Test matrix:
#   /codebooks/
#     - with data       → codebook names and authors appear in the table
#     - empty list      → "No codebooks found" message shown
#     - backend error   → error message rendered in an alert (page still returns 200)
#
#   /codebooks/<id>/themes
#     - with data       → theme name appears (embedded as JSON in data- attrs and in page)
#     - name query param → ?name=&version= values rendered in the page header
#     - empty data      → "No themes found" message shown
#     - backend error   → error message rendered in an alert (page still returns 200)
#
# Note: the theme browser JS (codebook_themes.js) is not executed during these tests —
# Flask's test client returns server-rendered HTML only. JS behaviour must be verified
# manually in a browser.

# GET /codebooks/


def test_codebook_list_renders_codebooks(client, fake_backend):
    fake_backend.codebooks = [
        {"id": "cb-1", "name": "Interview Codebook", "version": 1,
         "project_id": "proj-1", "created_by": "alice", "description": None},
        {"id": "cb-2", "name": "Focus Group Codebook", "version": 2,
         "project_id": "proj-1", "created_by": "bob", "description": None},
    ]
    resp = client.get("/codebooks/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Interview Codebook" in resp.data
    assert b"Focus Group Codebook" in resp.data
    assert b"alice" in resp.data
    assert b"bob" in resp.data
    assert b'id="corpus_selector"' in resp.data


def test_codebook_list_renders_empty_state(client, fake_backend):
    fake_backend.codebooks = []
    resp = client.get("/codebooks/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"No codebooks found" in resp.data


def test_codebook_list_renders_backend_error(client, fake_backend):
    fake_backend.raise_on = "list_codebooks"
    resp = client.get("/codebooks/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"simulated list_codebooks failure" in resp.data
    # Never leak internals.
    assert b"Traceback" not in resp.data
    assert b"BackendError" not in resp.data


def test_codebook_list_shows_unavailable_message_when_backend_down(client, fake_backend):
    from web.services.backend_client import BackendUnavailableError

    fake_backend.raise_on = ("list_codebooks", BackendUnavailableError)
    resp = client.get("/codebooks/", follow_redirects=True)
    assert resp.status_code == 200
    # Substring chosen to avoid the apostrophe in "can't", which Jinja2
    # HTML-escapes to "can&#39;t" when rendering the flash message.
    assert b"reach the analysis service" in resp.data
    # Empty-state line must NOT appear alongside the error.
    assert b"No codebooks found" not in resp.data


# GET /codebooks/<id>/themes



def test_codebook_themes_renders_frequency_and_tree(client, fake_backend):
    fake_backend.theme_frequencies = [
        {"theme_id": "t-1", "theme_name": "Work-Life Balance",
         "occurrence_count": 5, "interview_coverage_percentage": 60.0},
    ]
    fake_backend.theme_tree = [
        {"theme": {"id": "t-1", "label": "Work-Life Balance", "is_active": True},
         "children": []},
    ]
    resp = client.get("/codebooks/cb-1/themes", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Work-Life Balance" in resp.data
    assert b'id="corpus_selector"' in resp.data


def test_codebook_themes_renders_name_from_query_param(client, fake_backend):
    fake_backend.theme_frequencies = []
    fake_backend.theme_tree = []
    resp = client.get("/codebooks/cb-1/themes?name=My+Codebook&version=3", follow_redirects=True)
    assert resp.status_code == 200
    assert b"My Codebook" in resp.data


def test_codebook_themes_renders_empty_state(client, fake_backend):
    fake_backend.theme_frequencies = []
    fake_backend.theme_tree = []
    resp = client.get("/codebooks/cb-1/themes", follow_redirects=True)
    assert resp.status_code == 200
    assert b"No themes found" in resp.data


def test_codebook_themes_renders_backend_error(client, fake_backend):
    fake_backend.raise_on = "get_theme_frequencies"
    resp = client.get("/codebooks/cb-1/themes", follow_redirects=True)
    assert resp.status_code == 200
    assert b"simulated get_theme_frequencies failure" in resp.data
    assert b"Traceback" not in resp.data


def test_codebook_themes_shows_not_found_message_for_missing_codebook(client, fake_backend):
    from web.services.backend_client import BackendNotFoundError

    fake_backend.raise_on = ("get_theme_frequencies", BackendNotFoundError)
    resp = client.get("/codebooks/missing-id/themes", follow_redirects=True)
    assert resp.status_code == 200
    # Substring avoids apostrophe in "couldn't" — Jinja2 HTML-escapes it.
    assert b"may have been deleted" in resp.data
    # Empty-state line must NOT appear alongside the error.
    assert b"No themes found for this codebook" not in resp.data
