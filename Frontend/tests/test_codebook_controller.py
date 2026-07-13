"""Unit/integration tests for the codebook frontend Blueprint/controller.

Mocks the BackendClient to isolate Flask template rendering and controller behavior.
"""
from __future__ import annotations

import io
import zipfile
import pytest
from web.services.backend_client import BackendError


class FakeCodebookBackend:
    """Standalone Mock BackendClient for codebook controller testing."""

    def __init__(self) -> None:
        self.parse_csv_results = []
        self.create_codebook_result = {}
        self.get_codebook_result = {}
        self.get_codebook_results: dict[str, dict] = {}
        self.list_codebooks_result = []
        self.raise_on = None

        self.last_parsed_file = None
        self.last_created_payload = None
        self.last_fetched_id = None
        self.deleted_ids: list[str] = []

    def parse_csv_preview(self, file) -> list[dict]:
        self._maybe_raise("parse_csv_preview")
        self.last_parsed_file = file.filename
        return self.parse_csv_results

    def create_codebook(self, *, corpus_id: str, name: str, themes: list[dict]) -> dict:
        self._maybe_raise("create_codebook")
        self.last_created_payload = {"corpus_id": corpus_id, "name": name, "themes": themes}
        return self.create_codebook_result

    def get_codebook(self, codebook_id: str) -> dict:
        self._maybe_raise("get_codebook")
        self.last_fetched_id = codebook_id
        if codebook_id in self.get_codebook_results:
            return self.get_codebook_results[codebook_id]
        return self.get_codebook_result

    def delete_codebook(self, codebook_id: str) -> None:
        self._maybe_raise("delete_codebook")
        self.deleted_ids.append(codebook_id)

    def list_codebooks(self, corpus_id: str | None = None) -> list[dict]:
        self._maybe_raise("list_codebooks")
        return self.list_codebooks_result

    def list_generation_jobs(
        self, corpus_id: str, statuses: list[str] | None = None
    ) -> list[dict]:
        self._maybe_raise("list_generation_jobs")
        return []

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
# GET /codebooks/upload  — landing now redirects to the unified upload page
# ---------------------------------------------------------------------------


def test_codebooks_upload_landing_redirects_to_unified(client, fake_codebook_backend):
    resp = client.get("/codebooks/upload")
    assert resp.status_code == 302
    assert _UNIFIED_UPLOAD_PATH in resp.headers["Location"]
    assert "focus=codebook" in resp.headers["Location"]


def test_codebooks_upload_landing_backend_error_redirects_to_list(client, fake_codebook_backend):
    fake_codebook_backend.raise_on = "list_corpora"
    resp = client.get("/codebooks/upload")
    assert resp.status_code == 302
    assert "/codebooks/" in resp.headers["Location"]
    assert _UNIFIED_UPLOAD_PATH not in resp.headers["Location"]


def test_unified_upload_page_shows_codebook_card(client, fake_backend):
    resp = client.get(_UNIFIED_UPLOAD_PATH)
    assert resp.status_code == 200
    assert b"Upload Codebook" in resp.data
    assert b"CSV" in resp.data

    assert b"Generate" in resp.data
    assert b"enter manually" in resp.data
    assert b"/codebooks/new/test-corpus-id" in resp.data
    assert b"/codebooks/test-corpus-id/manual" in resp.data


# ---------------------------------------------------------------------------
# GET /codebooks/manual  — manual entry (now a proper GET, no loop)
# ---------------------------------------------------------------------------


def test_manual_form_renders_blank_row(client, fake_codebook_backend):
    resp = client.get("/codebooks/test-corpus-id/manual")
    assert resp.status_code == 200
    assert b"Create Codebook" in resp.data
    assert b'name="row_names[]"' in resp.data
    assert b'name="row_descriptions[]"' in resp.data
    assert b'action="/codebooks/test-corpus-id/confirm"' in resp.data


# ---------------------------------------------------------------------------
# POST /codebooks/new/<corpus_id>  — mode selection submit
# ---------------------------------------------------------------------------


def test_mode_submit_manual_redirects_to_unified(client):
    resp = client.post(
        "/codebooks/new/test-corpus-id",
        data={"mode": "manual"},
    )
    assert resp.status_code == 302
    assert _UNIFIED_UPLOAD_PATH in resp.headers["Location"]
    assert "focus=codebook" in resp.headers["Location"]


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
    assert b"Create Codebook" in resp.data
    assert b"Theme A" in resp.data
    assert b"Theme B" in resp.data
    assert fake_codebook_backend.last_parsed_file == "my_codebook.csv"


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
            "row_names[]": ["Theme 1", "Theme 2"],
            "row_descriptions[]": ["Desc 1", "Desc 2"],
            "row_parents[]": ["", ""],
            "row_is_codes[]": ["0", "0"],
        },
    )

    assert resp.status_code == 302
    assert "codebook_id=e2f1ad9a-6ab3-4df4-a3f2-c3a2f8b5a002" in resp.headers["Location"]
    payload = fake_codebook_backend.last_created_payload
    assert payload["name"] == "Verified Codebook"
    assert len(payload["themes"]) == 2
    assert payload["themes"][0]["name"] == "Theme 1"
    # Both root rows derive to THEME.
    assert payload["themes"][0]["node_type"] == "THEME"
    # No source draft -> nothing deleted.
    assert fake_codebook_backend.deleted_ids == []


def test_confirm_submit_deletes_draft_after_edit(client, fake_codebook_backend):
    # Edited draft (name differs) -> new codebook created and draft deleted.
    fake_codebook_backend.get_codebook_result = {"name": "Original Draft", "themes": []}
    fake_codebook_backend.create_codebook_result = {"id": "new-id", "name": "Edited Codebook"}

    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Edited Codebook",
            "row_names[]": ["Theme 1"],
            "row_descriptions[]": ["Desc 1"],
            "row_parents[]": [""],
            "row_is_codes[]": ["0"],
            "source_codebook_id": "draft-id",
        },
    )

    assert resp.status_code == 302
    assert "codebook_id=new-id" in resp.headers["Location"]
    assert fake_codebook_backend.deleted_ids == ["draft-id"]


def test_confirm_submit_edit_survives_failed_draft_cleanup(client, fake_codebook_backend):
    # A failed draft delete must not break the flow (best-effort cleanup).
    fake_codebook_backend.get_codebook_result = {"name": "Original Draft", "themes": []}
    fake_codebook_backend.create_codebook_result = {"id": "new-id", "name": "Edited"}
    fake_codebook_backend.raise_on = "delete_codebook"

    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Edited",
            "row_names[]": ["Theme 1"],
            "row_descriptions[]": ["Desc 1"],
            "row_parents[]": [""],
            "row_is_codes[]": ["0"],
            "source_codebook_id": "draft-id",
        },
    )

    assert resp.status_code == 302
    assert "codebook_id=new-id" in resp.headers["Location"]


def test_confirm_submit_validation_missing_name(client):
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "",
            "row_names[]": ["T1"],
            "row_descriptions[]": ["D1"],
            "row_parents[]": [""],
            "row_is_codes[]": ["0"],
        },
    )
    assert resp.status_code == 200
    assert b"Codebook name must not be blank" in resp.data


def test_confirm_submit_validation_blank_row_names(client):
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "row_names[]": ["", "Valid"],
            "row_descriptions[]": ["D1", "D2"],
            "row_parents[]": ["", ""],
            "row_is_codes[]": ["0", "0"],
        },
    )
    assert resp.status_code == 200
    assert b"All rows must have a name" in resp.data


def test_confirm_submit_surfaces_backend_error(client, fake_codebook_backend):
    fake_codebook_backend.raise_on = "create_codebook"
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "row_names[]": ["Theme A"],
            "row_descriptions[]": ["Desc A"],
            "row_parents[]": [""],
            "row_is_codes[]": ["0"],
        },
    )
    assert resp.status_code == 200
    assert b"simulated create_codebook failure" in resp.data


def test_confirm_submit_validation_parentless_code(client):
    # The type is derived server-side; a code with no parent is invalid.
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "row_names[]": ["Lonely code"],
            "row_descriptions[]": ["Desc"],
            "row_parents[]": [""],
            "row_is_codes[]": ["1"],
        },
    )
    assert resp.status_code == 200
    assert b"codes must sit under a theme or subtheme" in resp.data


def test_confirm_submit_validation_code_with_children(client):
    # Codes must be leaves: a row nested under a code is rejected.
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "row_names[]": ["Root", "Parent code", "Child"],
            "row_descriptions[]": ["a", "b", "c"],
            "row_parents[]": ["", "Root", "Parent code"],
            "row_is_codes[]": ["0", "1", "0"],
        },
    )
    assert resp.status_code == 200
    assert b"codes must be leaf nodes" in resp.data


def test_confirm_submit_validation_parent_does_not_exist(client):
    resp = client.post(
        "/codebooks/test-corpus-id/confirm",
        data={
            "codebook_name": "Tst",
            "row_names[]": ["Theme A", "Sub A"],
            "row_descriptions[]": ["Desc A", "Desc Sub A"],
            "row_parents[]": ["", "Unknown Theme"],
            "row_is_codes[]": ["0", "0"],
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


def test_success_no_codebook_id_redirects_to_unified(client):
    resp = client.get("/codebooks/test-corpus-id/success")
    assert resp.status_code == 302
    assert _UNIFIED_UPLOAD_PATH in resp.headers["Location"]
    assert "focus=codebook" in resp.headers["Location"]


def test_success_backend_error_shows_try_again_to_unified(client, fake_codebook_backend):
    fake_codebook_backend.raise_on = "get_codebook"
    resp = client.get("/codebooks/test-corpus-id/success?codebook_id=bad-id")
    assert resp.status_code == 200
    assert b"Try Again" in resp.data
    assert _UNIFIED_UPLOAD_PATH.encode() in resp.data
    assert b"codebooks/test-corpus-id/upload" not in resp.data


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


def test_export_selected_codebooks_returns_zip(client, fake_codebook_backend):
    fake_codebook_backend.get_codebook_results = {
        "cb-1": {
            "id": "cb-1",
            "name": "First Codebook",
            "version": 1,
            "themes": [
                {
                    "node_type": "THEME",
                    "name": "Theme A",
                    "description": "Desc A",
                    "children": [],
                }
            ],
        },
        "cb-2": {
            "id": "cb-2",
            "name": "Second Codebook",
            "version": 2,
            "themes": [
                {
                    "node_type": "THEME",
                    "name": "Theme B",
                    "description": "Desc B",
                    "children": [],
                }
            ],
        },
    }

    resp = client.post(
        "/codebooks/test-corpus-id/export",
        data={"item_ids": ["cb-1", "cb-2"]},
    )

    assert resp.status_code == 200
    assert resp.mimetype == "application/zip"
    assert "attachment; filename=selected_codebooks.zip" in resp.headers["Content-Disposition"]

    with zipfile.ZipFile(io.BytesIO(resp.data)) as archive:
        assert set(archive.namelist()) == {
            "First_Codebook_v1.csv",
            "Second_Codebook_v2.csv",
        }
        assert "THEME,Theme A,Desc A," in archive.read("First_Codebook_v1.csv").decode("utf-8")
        assert "THEME,Theme B,Desc B," in archive.read("Second_Codebook_v2.csv").decode("utf-8")


def test_export_selected_codebooks_requires_selection(client, fake_codebook_backend):
    resp = client.post(
        "/codebooks/test-corpus-id/export",
        data={},
        follow_redirects=True,
    )

    assert resp.status_code == 200
    assert b"Select at least one codebook to export" in resp.data
