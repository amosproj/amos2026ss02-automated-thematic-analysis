# conftest.py — shared pytest fixtures for the Frontend test suite.
#
# pytest discovers this file automatically (no import needed in test files).
# Every fixture defined here is available to all tests under Frontend/tests/.
#
# Fixtures provided:
#   app          — creates a Flask application instance via the app factory.
#   client       — a Flask test client that makes HTTP requests without a real server.
#   fake_backend — a FakeBackend instance wired into both controller modules via
#                  monkeypatch, replacing the real BackendClient so tests never hit
#                  the actual FastAPI backend.
#
# Usage in a test:
#   def test_something(client, fake_backend):
#       fake_backend.codebooks = [{"id": "cb-1", "name": "My Codebook", ...}]
#       resp = client.get("/codebooks/")
#       assert b"My Codebook" in resp.data
#
# To simulate a backend error, set raise_on to the method name:
#   fake_backend.raise_on = "list_codebooks"
#       > raises generic BackendError
#   fake_backend.raise_on = ("list_codebooks", BackendUnavailableError)
#       > raises that specific subclass (with its default user_message)

import pytest

from web import create_app


class FakeBackend:
    """Stand-in for `BackendClient`. Tests set data fields / `raise_on` before
    triggering a request."""

    def __init__(self) -> None:
        self.corpus_id = "test-corpus-id"
        self.documents: list[dict] = []
        self.upload_results: list[dict] = []
        self.uploaded_files: list[str] = []
        self.codebooks: list[dict] = []
        self.theme_frequencies: list[dict] = []
        self.theme_tree: list[dict] = []
        # Demographic data
        self.demographic_files: list[dict] = []
        self.demographic_rows: list[dict] = []
        self.demographic_link_summary: dict = {
            "total_transcripts": 0,
            "matched": 0,
            "details": [],
        }
        self.demographic_upload_response: dict | None = None
        self.demographic_confirm_response: dict | None = None
        # Either a method-name string (generic BackendError) or a
        # (method-name, ExceptionClass) tuple (specific typed subclass).
        self.raise_on: str | tuple[str, type] | None = None

    # ---- Corpora / documents ------------------------------------------------

    def ensure_corpus(self, project_id: str, name: str) -> str:
        self._maybe_raise("ensure_corpus")
        return self.corpus_id

    def upload_files(self, corpus_id, files) -> list[dict]:
        self._maybe_raise("upload_files")
        self.uploaded_files = [f.filename for f in files]
        return self.upload_results

    def list_documents(self, corpus_id, page_size: int = 50) -> list[dict]:
        self._maybe_raise("list_documents")
        return self.documents

    # ---- Codebooks / themes -------------------------------------------------

    def list_codebooks(self) -> list[dict]:
        self._maybe_raise("list_codebooks")
        return self.codebooks

    def get_theme_frequencies(self, codebook_id: str) -> list[dict]:
        self._maybe_raise("get_theme_frequencies")
        return self.theme_frequencies

    def get_theme_tree(self, codebook_id: str) -> list[dict]:
        self._maybe_raise("get_theme_tree")
        return self.theme_tree

    # ---- Demographic --------------------------------------------------------

    def upload_demographic(self, corpus_id, file, name=None) -> dict:
        self._maybe_raise("upload_demographic")
        return self.demographic_upload_response or {}

    def confirm_demographic(self, corpus_id, import_id, confirm) -> dict:
        self._maybe_raise("confirm_demographic")
        return self.demographic_confirm_response or {}

    def list_demographic_files(self, corpus_id, page_size=200) -> list[dict]:
        self._maybe_raise("list_demographic_files")
        return self.demographic_files

    def list_demographic_rows(self, corpus_id, file_id, page=1, page_size=200) -> list[dict]:
        self._maybe_raise("list_demographic_rows")
        return self.demographic_rows

    def get_demographic_link_summary(self, corpus_id) -> dict:
        self._maybe_raise("get_demographic_link_summary")
        return self.demographic_link_summary

    # ---- Internal -----------------------------------------------------------

    def _maybe_raise(self, method: str) -> None:
        from web.services.backend_client import BackendError

        if self.raise_on == method:
            raise BackendError(f"simulated {method} failure")
        if (
            isinstance(self.raise_on, tuple)
            and len(self.raise_on) == 2
            and self.raise_on[0] == method
        ):
            exc_class = self.raise_on[1]
            raise exc_class()


@pytest.fixture
def fake_backend(monkeypatch) -> FakeBackend:
    """Patches all controllers' `_backend()` factories to return one FakeBackend."""
    fake = FakeBackend()
    monkeypatch.setattr("web.controllers.ingestion._backend", lambda: fake)
    monkeypatch.setattr("web.controllers.codebooks._backend", lambda: fake)
    monkeypatch.setattr("web.controllers.demographic._backend", lambda: fake)
    return fake


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
def client(app):
    return app.test_client()
