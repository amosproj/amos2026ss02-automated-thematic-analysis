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
#   fake_backend.raise_on = "list_codebooks"  # that call will raise BackendError

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
        self.raise_on: str | None = None

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

    # ---- Internal -----------------------------------------------------------

    def _maybe_raise(self, method: str) -> None:
        if self.raise_on == method:
            from web.services.backend_client import BackendError
            raise BackendError(f"simulated {method} failure")


@pytest.fixture
def fake_backend(monkeypatch) -> FakeBackend:
    """Patches all controllers' `_backend()` factories to return one FakeBackend."""
    fake = FakeBackend()
    monkeypatch.setattr("web.controllers.ingestion._backend", lambda: fake)
    monkeypatch.setattr("web.controllers.codebooks._backend", lambda: fake)
    return fake


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
def client(app):
    return app.test_client()
