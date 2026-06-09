"""Unit/integration tests for the codebook frontend Blueprint/controller.

Mocks the BackendClient to isolate Flask template rendering and controller behavior.
"""
from __future__ import annotations

import io
import pytest
from web.services.backend_client import BackendError


class FakeCodebookBackend:
    """Standalone Mock BackendClient for codebook controller testing."""

    def __init__(self) -> None:
        self.parse_csv_results = []
        self.create_codebook_result = {}
        self.get_codebook_result = {}
        self.list_codebooks_result = []
        self.raise_on = None

        self.last_parsed_file = None
        self.last_created_payload = None
        self.last_fetched_id = None

    def parse_csv_preview(self, file) -> list[dict]:
        self._maybe_raise("parse_csv_preview")
        self.last_parsed_file = file.filename
        return self.parse_csv_results

    def create_codebook(self, project_id: str, name: str, themes: list[dict]) -> dict:
        self._maybe_raise("create_codebook")
        self.last_created_payload = {"project_id": project_id, "name": name, "themes": themes}
        return self.create_codebook_result

    def get_codebook(self, codebook_id: str) -> dict:
        self._maybe_raise("get_codebook")
        self.last_fetched_id = codebook_id
        return self.get_codebook_result

    def list_codebooks(self, corpus_id: str | None = None) -> list[dict]:
        self._maybe_raise("list_codebooks")
        return self.list_codebooks_result

    def list_corpora(self, corpus_id: str | None = None) -> list[dict]:
        self._maybe_raise("list_corpora")
        return [{"id": "test-corpus-id", "name": "Default Corpus"}]

    def ensure_corpus(self, corpus_id: str, name: str) -> str:
        self._maybe_raise("ensure_corpus")
        return corpus_id

    def _maybe_raise(self, method: str) -> None:
        if self.raise_on == method:
            raise BackendError(f"simulated {method} failure")


@pytest.fixture
def fake_codebook_backend(monkeypatch) -> FakeCodebookBackend:
    fake = FakeCodebookBackend()
    monkeypatch.setattr("web.controllers.codebooks._backend", lambda: fake)
    return fake


# ---------------------------------------------------------------------------
# GET /codebooks/   — list view
# ---------------------------------------------------------------------------


def test_list_codebooks_renders_empty_state(client, fake_codebook_backend):
    fake_codebook_backend.list_codebooks_result = []
    resp = client.get("/codebooks/test-corpus-id/")
    assert resp.status_code == 200
    assert b"No codebooks found" in resp.data


def test_list_codebooks_renders_saved_entries(client, fake_codebook_backend):
    fake_codebook_backend.list_codebooks_result = [
        {"id": "abc-1", "name": "Interview Framework", "version": 1,
         "project_id": "default-project", "created_by": "researcher"},
        {"id": "abc-2", "name": "Health Study", "version": 2,
         "project_id": "default-project", "created_by": "researcher"},
    ]
    resp = client.get("/codebooks/test-corpus-id/")
    assert resp.status_code == 200
    assert b"Interview Framework" in resp.data
    assert b"Health Study" in resp.data
    assert b"No codebooks found" not in resp.data


def test_list_codebooks_surfaces_backend_error(client, fake_codebook_backend):
    fake_codebook_backend.raise_on = "list_codebooks"
    resp = client.get("/codebooks/test-corpus-id/")
    assert resp.status_code == 200
    assert b"simulated list_codebooks failure" in resp.data


# ---------------------------------------------------------------------------
# GET /codebooks/upload
# ---------------------------------------------------------------------------


def test_upload_form_renders_correctly(client):
    resp = client.get("/codebooks/test-corpus-id/upload")
    assert resp.status_code == 200
    assert b"Upload" in resp.data
    assert b"CSV" in resp.data


# ---------------------------------------------------------------------------
# GET /codebooks/manual  — manual entry (now a proper GET, no loop)
# ---------------------------------------------------------------------------


def test_manual_form_renders_blank_row(client, fake_codebook_backend):
    resp = client.get("/codebooks/test-corpus-id/manual")
    assert resp.status_code == 200
    assert b"Preview &amp; Confirm Themes" in resp.data
    assert b'name="theme_names[]"' in resp.data
    assert b'name="theme_descriptions[]"' in resp.data


# ---------------------------------------------------------------------------
# POST /codebooks/upload
# ---------------------------------------------------------------------------


def test_upload_submit_csv_success(client, fake_codebook_backend):
    fake_codebook_backend.parse_csv_results = [
        {"node_type": "THEME", "name": "Theme A", "description": "Desc A", "parent_name": ""},
        {"node_type": "THEME", "name": "Theme B", "description": "Desc B", "parent_name": ""},
    ]

    resp = client.post(
        "/codebooks/test-corpus-id/upload",
        data={
            "file": (io.BytesIO(b"name,description\nTheme A,Desc A\nTheme B,Desc B"), "my_codebook.csv"),
            "action": "upload",
        },
        content_type="multipart/form-data",
    )

    assert resp.status_code == 200
    assert b"Preview &amp; Confirm Themes" in resp.data
    assert b"Theme A" in resp.data
    assert b"Theme B" in resp.data
    assert fake_codebook_backend.last_parsed_file == "my_codebook.csv"


def test_upload_submit_manual_redirects_to_manual_form(client, fake_codebook_backend):
    """POST with action=manual should redirect to GET /codebooks/manual (not render inline)."""
    resp = client.post(
        "/codebooks/test-corpus-id/upload",
        data={"action": "manual"},
    )
    assert resp.status_code == 302
    assert "/codebooks/test-corpus-id/manual" in resp.headers["Location"]


# On error the codebook upload now redirects back to the unified upload page
# (ingestion.upload_form, mounted at /transcripts/<corpus_id>/upload) with a
# flashed message, rather than rendering the standalone codebooks/upload.html.

_UNIFIED_UPLOAD_PATH = "/transcripts/test-corpus-id/upload"


def _session_flashes(client) -> list[tuple[str, str]]:
    with client.session_transaction() as sess:
        return list(sess.get("_flashes", []))


def test_upload_submit_no_file_redirects_to_unified_with_flash(client):
    resp = client.post(
        "/codebooks/test-corpus-id/upload",
        data={"action": "upload"},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302
    assert _UNIFIED_UPLOAD_PATH in resp.headers["Location"]
    assert any("Please select a CSV file" in msg for _, msg in _session_flashes(client))


def test_upload_submit_invalid_extension_redirects_to_unified_with_flash(client):
    resp = client.post(
        "/codebooks/test-corpus-id/upload",
        data={
            "file": (io.BytesIO(b"stuff"), "codebook.txt"),
            "action": "upload",
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302
    assert _UNIFIED_UPLOAD_PATH in resp.headers["Location"]
    assert any("Only CSV files" in msg for _, msg in _session_flashes(client))


def test_upload_submit_surfaces_backend_parse_error(client, fake_codebook_backend):
    fake_codebook_backend.raise_on = "parse_csv_preview"
    resp = client.post(
        "/codebooks/test-corpus-id/upload",
        data={
            "file": (io.BytesIO(b"bad_csv"), "codebook.csv"),
            "action": "upload",
        },
        content_type="multipart/form-data",
    )
    assert resp.status_code == 302
    assert _UNIFIED_UPLOAD_PATH in resp.headers["Location"]
    assert any("simulated parse_csv_preview failure" in msg for _, msg in _session_flashes(client))


# ---------------------------------------------------------------------------
# POST /codebooks/test-corpus-id/confirm
# ---------------------------------------------------------------------------


def test_confirm_submit_success(client, fake_codebook_backend):
    fake_codebook_backend.create_codebook_result = {
        "id": "e2f1ad9a-6ab3-4df4-a3f2-c3a2f8b5a002",
        "name": "Persisted Codebook",
    }

    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Verified Codebook",
            "node_types[]": ["THEME", "THEME"],
            "theme_names[]": ["Theme 1", "Theme 2"],
            "theme_descriptions[]": ["Desc 1", "Desc 2"],
            "parent_names[]": ["", ""],
        },
    )

    assert resp.status_code == 302
    assert "codebook_id=e2f1ad9a-6ab3-4df4-a3f2-c3a2f8b5a002" in resp.headers["Location"]
    payload = fake_codebook_backend.last_created_payload
    assert payload["name"] == "Verified Codebook"
    assert len(payload["themes"]) == 2
    assert payload["themes"][0]["name"] == "Theme 1"


def test_confirm_submit_validation_missing_name(client):
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "",
            "node_types[]": ["THEME"],
            "theme_names[]": ["T1"],
            "theme_descriptions[]": ["D1"],
            "parent_names[]": [""],
        },
    )
    assert resp.status_code == 200
    assert b"Codebook Name must not be blank" in resp.data


def test_confirm_submit_validation_blank_theme_names(client):
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "node_types[]": ["THEME", "THEME"],
            "theme_names[]": ["", "Valid"],
            "theme_descriptions[]": ["D1", "D2"],
            "parent_names[]": ["", ""],
        },
    )
    assert resp.status_code == 200
    assert b"All themes must have a name" in resp.data


def test_confirm_submit_surfaces_backend_error(client, fake_codebook_backend):
    fake_codebook_backend.raise_on = "create_codebook"
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "node_types[]": ["THEME"],
            "theme_names[]": ["Theme A"],
            "theme_descriptions[]": ["Desc A"],
            "parent_names[]": [""],
        },
    )
    assert resp.status_code == 200
    assert b"simulated create_codebook failure" in resp.data

def test_confirm_submit_validation_missing_parent(client):
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "node_types[]": ["THEME", "SUBTHEME"],
            "theme_names[]": ["Theme A", "Sub A"],
            "theme_descriptions[]": ["Desc A", "Desc Sub A"],
            "parent_names[]": ["", ""],
        },
    )
    assert resp.status_code == 200
    assert b"must have a Parent Name" in resp.data

def test_confirm_submit_validation_theme_has_parent(client):
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "node_types[]": ["THEME", "THEME"],
            "theme_names[]": ["Theme A", "Theme B"],
            "theme_descriptions[]": ["Desc A", "Desc B"],
            "parent_names[]": ["", "Theme A"],
        },
    )
    assert resp.status_code == 200
    assert b"must not have a Parent Name" in resp.data

def test_confirm_submit_validation_parent_does_not_exist(client):
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "node_types[]": ["THEME", "SUBTHEME"],
            "theme_names[]": ["Theme A", "Sub A"],
            "theme_descriptions[]": ["Desc A", "Desc Sub A"],
            "parent_names[]": ["", "Unknown Theme"],
        },
    )
    assert resp.status_code == 200
    assert b"does not exist in this codebook" in resp.data


# ---------------------------------------------------------------------------
# GET /codebooks/success
# ---------------------------------------------------------------------------


def test_success_renders_saved_details(client, fake_codebook_backend):
    fake_codebook_backend.get_codebook_result = {
        "id": "e2f1ad9a-6ab3-4df4-a3f2-c3a2f8b5a002",
        "name": "Success Codebook",
        "project_id": "proj-123",
        "version": 2,
        "created_by": "researcher",
        "themes": [
            {"id": "t1", "name": "Theme A", "description": "D A"},
        ],
    }

    resp = client.get("/codebooks/test-corpus-id/success?codebook_id=e2f1ad9a-6ab3-4df4-a3f2-c3a2f8b5a002")

    assert resp.status_code == 200
    assert b"Codebook Saved Successfully" in resp.data
    assert b"Success Codebook" in resp.data
    assert b"Theme A" in resp.data
    assert fake_codebook_backend.last_fetched_id == "e2f1ad9a-6ab3-4df4-a3f2-c3a2f8b5a002"

# ---------------------------------------------------------------------------
# GET /codebooks/<codebook_id>/export
# ---------------------------------------------------------------------------

def test_export_codebook_success(client, fake_codebook_backend):
    fake_codebook_backend.get_codebook_result = {
        "id": "e2f1ad9a-6ab3-4df4-a3f2-c3a2f8b5a002",
        "name": "Export Codebook",
        "version": 1,
        "themes": [
            {
                "node_type": "THEME",
                "name": "Theme A",
                "description": "Desc A",
                "children": [
                    {
                        "node_type": "SUBTHEME",
                        "name": "Sub A1",
                        "description": "Desc A1",
                        "children": []
                    }
                ]
            }
        ]
    }

    resp = client.get("/codebooks/test-corpus-id/e2f1ad9a-6ab3-4df4-a3f2-c3a2f8b5a002/export")

    assert resp.status_code == 200
    assert resp.mimetype == "text/csv"
    assert "attachment; filename=Export_Codebook_v1.csv" in resp.headers["Content-Disposition"]
    
    # Check CSV contents
    csv_data = resp.data.decode("utf-8")
    assert "Node Type,Name,Description,Parent Name" in csv_data
    assert "THEME,Theme A,Desc A," in csv_data
    assert "SUBTHEME,Sub A1,Desc A1,Theme A" in csv_data

def test_export_codebook_not_found(client, fake_codebook_backend):
    fake_codebook_backend.raise_on = "get_codebook"
    resp = client.get("/codebooks/test-corpus-id/unknown-id/export")
    assert resp.status_code == 302
    assert "/codebooks/" in resp.headers["Location"]
