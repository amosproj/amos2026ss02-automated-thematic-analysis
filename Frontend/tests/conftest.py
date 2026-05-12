import pytest

from web import create_app


class FakeBackend:
    """Stand-in for `BackendClient`. Tests set `documents`/`upload_results`/
    `raise_on` before triggering a request."""

    def __init__(self) -> None:
        self.corpus_id = "test-corpus-id"
        self.documents: list[dict] = []
        self.upload_results: list[dict] = []
        self.uploaded_files: list[str] = []
        self.raise_on: str | None = None

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

    def _maybe_raise(self, method: str) -> None:
        if self.raise_on == method:
            from web.services.backend_client import BackendError
            raise BackendError(f"simulated {method} failure")


@pytest.fixture
def fake_backend(monkeypatch) -> FakeBackend:
    """Patches the controller's `_backend()` factory to return a FakeBackend."""
    fake = FakeBackend()
    monkeypatch.setattr("web.controllers.ingestion._backend", lambda: fake)
    return fake


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
def client(app):
    return app.test_client()
