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
    assert b'id="global-corpus-select"' in resp.data


def test_codebook_list_renders_empty_state(client, fake_backend):
    fake_backend.codebooks = []
    resp = client.get("/codebooks/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"No codebooks found" in resp.data


# The "Create New Codebook" button is the wizard entry point. It lives in the
# page header outside the data/empty/error conditional, so one assertion is
# enough — the empty state is the cheapest path to render it.


def test_codebook_list_shows_create_button(client, fake_backend):
    fake_backend.codebooks = []
    resp = client.get("/codebooks/", follow_redirects=True)
    assert b"Create New Codebook" in resp.data
    assert b'href="/codebooks/new"' in resp.data


def test_codebook_list_row_shows_review_button(client, fake_backend):
    # Permanent path to the review editor for any codebook, so users can
    # always come back to edit even if they dismissed the completion toast.
    fake_backend.codebooks = [
        {"id": "cb-1", "name": "Interview Codebook", "version": 1,
         "project_id": "proj-1", "created_by": "alice", "description": None,
         "corpus_id": fake_backend.corpus_id},
    ]
    resp = client.get("/codebooks/", follow_redirects=True)
    assert b"Review &amp; edit" in resp.data
    assert b'href="/codebooks/cb-1/review"' in resp.data


def test_codebook_list_shows_running_job_row(client, fake_backend):
    # In-progress runs are server-rendered so they're visible in any browser
    # or session, not just the one that started the run.
    fake_backend.codebooks = []
    fake_backend.generation_jobs = {
        "job-1": {
            "id": "job-1", "status": "running",
            "codebook_name": "Interview Codebook",
            "corpus_id": fake_backend.corpus_id,
            "passages_total": 10, "passages_done": 3,
        },
    }
    resp = client.get("/codebooks/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Interview Codebook" in resp.data
    assert b"Running" in resp.data
    # Carries the hooks job_tracker.js polls and dedupes against.
    assert b'data-job-row="job-1"' in resp.data
    assert b'data-job-progress="job-1"' in resp.data


def test_codebook_list_survives_running_jobs_backend_error(client, fake_backend):
    # A failure fetching in-progress runs must not break the list page — the
    # client-side tracker remains as a fallback indicator.
    fake_backend.codebooks = [
        {"id": "cb-1", "name": "Interview Codebook", "version": 1,
         "project_id": "proj-1", "created_by": "alice", "description": None,
         "corpus_id": fake_backend.corpus_id},
    ]
    fake_backend.raise_on = "list_generation_jobs"
    resp = client.get("/codebooks/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Interview Codebook" in resp.data


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


def test_codebook_list_shows_error_when_requested_corpus_missing(client, fake_backend):
    fake_backend.corpora = [{"id": "existing-corpus", "name": "Existing Corpus"}]
    resp = client.get("/codebooks/missing-corpus/", follow_redirects=True)
    assert resp.status_code == 200
    assert b"selected corpus couldn" in resp.data
    assert b"No codebooks found" not in resp.data


# GET /codebooks/<id>/themes



def test_codebook_themes_renders_frequency_and_tree(client, fake_backend):
    fake_backend.codebooks = [
        {"id": "cb-1", "name": "Interview Codebook", "version": 1,
         "project_id": "proj-1", "created_by": "alice", "description": None,
         "corpus_id": fake_backend.corpus_id},
    ]
    fake_backend.theme_frequencies = [
        {"theme_id": "t-1", "theme_name": "Work-Life Balance",
         "occurrence_count": 5, "interview_coverage_percentage": 60.0},
    ]
    fake_backend.theme_tree = [
        {"theme": {"id": "t-1", "label": "Work-Life Balance", "is_active": True},
         "children": []},
    ]
    resp = client.get("/codebooks/test-corpus-id/cb-1/themes", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Work-Life Balance" in resp.data
    assert b'id="global-corpus-select"' in resp.data


def test_codebook_themes_renders_name_from_query_param(client, fake_backend):
    fake_backend.codebooks = [
        {"id": "cb-1", "name": "Interview Codebook", "version": 3,
         "project_id": "proj-1", "created_by": "alice", "description": None,
         "corpus_id": fake_backend.corpus_id},
    ]
    fake_backend.theme_frequencies = []
    fake_backend.theme_tree = []
    resp = client.get("/codebooks/test-corpus-id/cb-1/themes?name=My+Codebook&version=3", follow_redirects=True)
    assert resp.status_code == 200
    assert b"My Codebook" in resp.data


def test_codebook_themes_renders_empty_state(client, fake_backend):
    fake_backend.codebooks = [
        {"id": "cb-1", "name": "Interview Codebook", "version": 1,
         "project_id": "proj-1", "created_by": "alice", "description": None,
         "corpus_id": fake_backend.corpus_id},
    ]
    fake_backend.theme_frequencies = []
    fake_backend.theme_tree = []
    resp = client.get("/codebooks/test-corpus-id/cb-1/themes", follow_redirects=True)
    assert resp.status_code == 200
    assert b"No themes found" in resp.data


def test_codebook_themes_renders_backend_error(client, fake_backend):
    fake_backend.codebooks = [
        {"id": "cb-1", "name": "Interview Codebook", "version": 1,
         "project_id": "proj-1", "created_by": "alice", "description": None,
         "corpus_id": fake_backend.corpus_id},
    ]
    fake_backend.raise_on = "get_theme_frequencies"
    resp = client.get("/codebooks/test-corpus-id/cb-1/themes", follow_redirects=True)
    assert resp.status_code == 200
    assert b"simulated get_theme_frequencies failure" in resp.data
    assert b"Traceback" not in resp.data


def test_codebook_themes_shows_not_found_message_for_missing_codebook(client, fake_backend):
    from web.services.backend_client import BackendNotFoundError

    fake_backend.codebooks = [
        {"id": "missing-id", "name": "Interview Codebook", "version": 1,
         "project_id": "proj-1", "created_by": "alice", "description": None,
         "corpus_id": fake_backend.corpus_id},
    ]
    fake_backend.raise_on = ("get_theme_frequencies", BackendNotFoundError)
    resp = client.get("/codebooks/test-corpus-id/missing-id/themes", follow_redirects=True)
    assert resp.status_code == 200
    # Substring avoids apostrophe in "couldn't" — Jinja2 HTML-escapes it.
    assert b"may have been deleted" in resp.data
    # Empty-state line must NOT appear alongside the error.
    assert b"No themes found for this codebook" not in resp.data


def test_codebook_themes_shows_error_for_codebook_outside_selected_corpus(client, fake_backend):
    fake_backend.codebooks = [
        {"id": "cb-2", "name": "Other Corpus Codebook", "version": 1,
         "project_id": "proj-1", "created_by": "bob", "description": None,
         "corpus_id": "other-corpus"},
    ]
    resp = client.get(f"/codebooks/{fake_backend.corpus_id}/cb-1/themes", follow_redirects=True)
    assert resp.status_code == 200
    assert b"selected corpus" in resp.data
    assert b"No themes found for this codebook" not in resp.data


# Wizard: Create New Codebook (step 1) ---------------------------------------


def test_new_codebook_landing_redirects_to_corpus_scoped_step1(client, fake_backend):
    fake_backend.corpus_id = "corpus-xyz"
    fake_backend.corpora = [{"id": "corpus-xyz", "name": "Test Corpus"}]
    resp = client.get("/codebooks/new")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/codebooks/new/corpus-xyz")


def test_new_codebook_landing_surfaces_backend_error(client, fake_backend):
    fake_backend.raise_on = "list_corpora"
    resp = client.get("/codebooks/new", follow_redirects=True)
    assert resp.status_code == 200
    assert b"simulated list_corpora failure" in resp.data


def test_new_codebook_mode_select_renders_three_cards(client, fake_backend):
    resp = client.get("/codebooks/new/corpus-xyz")
    assert resp.status_code == 200
    assert b"Fully automatic" in resp.data
    assert b"Semi-automatic" in resp.data
    assert b"User-instructed" in resp.data
    # `checked>` matches the server-rendered radio attribute and not JS like
    # `:checked` or `radio.checked` that also appears in the inline script.
    assert b"checked>" not in resp.data
    assert b'id="mode-submit"' in resp.data


def test_new_codebook_mode_select_pre_selects_from_query_param(client, fake_backend):
    resp = client.get("/codebooks/new/corpus-xyz?mode=semi")
    assert resp.status_code == 200
    assert resp.data.count(b"checked>") == 1
    assert b'value="semi"' in resp.data


def test_new_codebook_submit_without_mode_re_renders_with_flash(client, fake_backend):
    resp = client.post("/codebooks/new/corpus-xyz", data={})
    assert resp.status_code == 200
    assert b"Please select a coding mode" in resp.data
    assert b"Fully automatic" in resp.data


def test_new_codebook_submit_valid_mode_redirects_to_auto_form(client, fake_backend):
    # Both auto and semi proceed to the name form, carrying the chosen mode.
    for mode in ("auto", "semi"):
        resp = client.post("/codebooks/new/corpus-xyz", data={"mode": mode})
        assert resp.status_code == 302
        loc = resp.headers["Location"]
        assert "/codebooks/new/corpus-xyz/auto" in loc, mode
        assert f"mode={mode}" in loc


# Wizard step 2: name + confirm form (auto + semi) --------------------------


def test_auto_form_renders_with_mode_badge(client, fake_backend):
    resp = client.get("/codebooks/new/corpus-xyz/auto?mode=auto")
    assert resp.status_code == 200
    assert b"Fully automatic" in resp.data
    assert b"Generate codebook" in resp.data

    resp = client.get("/codebooks/new/corpus-xyz/auto?mode=semi")
    assert resp.status_code == 200
    assert b"Semi-automatic" in resp.data
    assert b"Generate &amp; review" in resp.data


def test_auto_form_defaults_invalid_mode_to_auto(client, fake_backend):
    resp = client.get("/codebooks/new/corpus-xyz/auto?mode=unknown")
    assert resp.status_code == 200
    assert b"Fully automatic" in resp.data


def test_auto_submit_without_name_re_renders_with_flash(client, fake_backend):
    resp = client.post(
        "/codebooks/new/corpus-xyz/auto", data={"mode": "auto", "codebook_name": "  "}
    )
    assert resp.status_code == 200
    assert b"give your codebook a name" in resp.data


def test_auto_submit_creates_job_and_redirects_to_codebook_list(client, fake_backend):
    resp = client.post(
        "/codebooks/new/corpus-xyz/auto",
        data={"mode": "auto", "codebook_name": "Interview Codebook"},
    )
    assert resp.status_code == 302
    loc = resp.headers["Location"]
    # New flow: handoff to the codebook list with new-job params so the
    # client-side tracker picks them up and polls in the background.
    assert "/codebooks/corpus-xyz/" in loc
    assert "new_job=job-1" in loc
    assert "mode=auto" in loc
    assert "name=Interview+Codebook" in loc
    assert fake_backend.last_generation_job_request == {
        "codebook_name": "Interview Codebook",
        "corpus_id": "corpus-xyz",
        "transcript_document_ids": None,
    }


def test_auto_submit_semi_redirects_to_progress_page(client, fake_backend):
    # Semi is a linear flow: land on the progress page (which auto-opens the
    # review editor on success), not the background-tracking list handoff.
    resp = client.post(
        "/codebooks/new/corpus-xyz/auto",
        data={"mode": "semi", "codebook_name": "Interview Codebook"},
    )
    assert resp.status_code == 302
    loc = resp.headers["Location"]
    assert "/codebooks/new/jobs/job-1" in loc
    assert "mode=semi" in loc
    assert "name=Interview+Codebook" in loc
    # No new_job handoff param — the page registers a localStorage fallback
    # only when the user navigates away (progress.html pagehide handler), so
    # job_tracker.js leaves the URL (and ?mode=semi) intact across reloads.
    assert "new_job=" not in loc


def test_auto_submit_surfaces_backend_error(client, fake_backend):
    fake_backend.raise_on = "create_generation_job"
    resp = client.post(
        "/codebooks/new/corpus-xyz/auto",
        data={"mode": "auto", "codebook_name": "X"},
    )
    assert resp.status_code == 200
    assert b"simulated create_generation_job failure" in resp.data


# Wizard step 3: progress page + JSON poller --------------------------------


def test_progress_page_renders_with_job_and_mode(client, fake_backend):
    resp = client.get("/codebooks/new/jobs/job-1?mode=semi")
    assert resp.status_code == 200
    assert b"Generating Codebook" in resp.data
    assert b'data-job-id="job-1"' in resp.data
    assert b'data-mode="semi"' in resp.data


def test_progress_status_returns_job_json(client, fake_backend):
    fake_backend.generation_jobs["job-7"] = {
        "id": "job-7", "status": "running",
        "passages_total": 10, "passages_done": 4,
    }
    resp = client.get("/codebooks/new/jobs/job-7.json")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "running"
    assert body["passages_done"] == 4


def test_progress_status_surfaces_backend_error_as_json(client, fake_backend):
    fake_backend.raise_on = "get_generation_job"
    resp = client.get("/codebooks/new/jobs/missing.json")
    assert resp.status_code == 200
    body = resp.get_json()
    assert "error" in body


def test_progress_cancel_returns_updated_job(client, fake_backend):
    fake_backend.generation_jobs["job-9"] = {
        "id": "job-9", "status": "running",
        "passages_total": 5, "passages_done": 1,
        "cancel_requested": False,
    }
    resp = client.post("/codebooks/new/jobs/job-9/cancel")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "cancelled"
    assert body["cancel_requested"] is True


# Mode-2 review — renders preview editor pre-filled from codebook tree -------


def test_review_renders_preview_editor_with_themes_subthemes_and_codes(client, fake_backend):
    # CodebookDetailSchema nests subthemes and codes inside theme children;
    # the controller walks this tree depth-first and produces preview.html rows
    # with node_type, parent_name, name, description.
    fake_backend.codebooks = [
        {"id": "cb-42", "name": "My Generated Codebook",
         "corpus_id": fake_backend.corpus_id, "version": 1,
         "created_by": "alice", "description": None,
         "themes": [
             {"id": "t-1", "node_type": "THEME", "name": "Work-Life Balance",
              "description": "Balance between work and personal life",
              "children": [
                  {"id": "t-2", "node_type": "SUBTHEME",
                   "name": "Boundary issues",
                   "description": "Difficulty separating",
                   "children": [
                       {"id": "c-1", "node_type": "CODE",
                        "name": "Late evenings",
                        "description": "Works past 8pm"},
                   ]},
              ]},
         ],
         "codes": []},
    ]
    resp = client.get("/codebooks/cb-42/review")
    assert resp.status_code == 200
    assert b"My Generated Codebook" in resp.data
    # All three node names appear in input fields.
    assert b'value="Work-Life Balance"' in resp.data
    assert b'value="Boundary issues"' in resp.data
    assert b'value="Late evenings"' in resp.data
    # Node type dropdowns are rendered for each row.
    assert b'name="node_types[]"' in resp.data
    # Parent name fields are pre-filled with the hierarchy.
    assert b'name="parent_names[]"' in resp.data
    assert b"SUBTHEME" in resp.data
    assert b"CODE" in resp.data


def test_review_not_found_for_missing_codebook(client, fake_backend):
    # No entry in fake_backend.codebooks; get_codebook raises NotFound.
    resp = client.get("/codebooks/missing/review")
    assert resp.status_code == 200
    assert b"Codebook not found" in resp.data


# Demo flow — exercises the wizard without an LLM call ---------------------


def test_auto_demo_creates_codebook_and_redirects_to_codebook_list(client, fake_backend):
    resp = client.get(f"/codebooks/new/{fake_backend.corpus_id}/auto-demo")
    assert resp.status_code == 302
    loc = resp.headers["Location"]
    # Demo flow uses the same background-tracking handoff as a real run.
    assert f"/codebooks/{fake_backend.corpus_id}/" in loc
    assert "new_job=demo-" in loc
    assert "mode=semi" in loc
    sent = fake_backend.last_create_codebook_request
    assert sent is not None
    assert sent["name"] == "Sample Codebook (demo)"
    # Realistic content: at least one theme with a subtheme, at least one CODE.
    themes = [n for n in sent["themes"] if n.get("node_type") != "CODE"]
    codes = [n for n in sent["themes"] if n.get("node_type") == "CODE"]
    assert any(t["parent_name"] for t in themes)
    assert codes


def test_auto_demo_status_progresses_to_succeeded(client, fake_backend):
    from urllib.parse import parse_qs, urlparse

    # Kick off a demo run.
    resp = client.get(f"/codebooks/new/{fake_backend.corpus_id}/auto-demo")
    qs = parse_qs(urlparse(resp.headers["Location"]).query)
    job_id = qs["new_job"][0]

    # Immediately after start, the scripted state is queued or running.
    first = client.get(f"/codebooks/new/jobs/{job_id}.json").get_json()
    assert first["status"] in ("queued", "running")

    # Fast-forward the demo's started_at so the timeline reads as finished.
    from web.controllers.codebooks import _DEMO_JOBS
    _DEMO_JOBS[job_id]["started_at"] -= 10  # 10s in the past

    final = client.get(f"/codebooks/new/jobs/{job_id}.json").get_json()
    assert final["status"] == "succeeded"
    assert final["codebook_id"] == "cb-new"  # FakeBackend.create_codebook returns this id


def test_auto_demo_link_present_on_mode_select(client, fake_backend):
    resp = client.get(f"/codebooks/new/{fake_backend.corpus_id}")
    assert resp.status_code == 200
    assert b"Try with sample data" in resp.data
    assert b"/auto-demo" in resp.data


