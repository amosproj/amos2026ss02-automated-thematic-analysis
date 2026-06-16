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
        self.corpora: list[dict] = [
            {"id": self.corpus_id, "name": "Default Corpus"},
        ]
        self.last_created_corpus: dict | None = None
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
        self.last_link_request: dict | None = None
        self.last_unlink_request: dict | None = None
        # Codebook generation jobs
        self.generation_jobs: dict[str, dict] = {}
        self.last_generation_job_request: dict | None = None
        self.last_create_codebook_request: dict | None = None
        # Either a method-name string (generic BackendError) or a
        # (method-name, ExceptionClass) tuple (specific typed subclass).
        self.raise_on: str | tuple[str, type] | None = None

    # ---- Corpora / documents ------------------------------------------------

    def ensure_corpus(self, corpus_id: str, name: str) -> str:
        self._maybe_raise("ensure_corpus")
        if not any(c.get("id") == corpus_id for c in self.corpora):
            self.corpora.append({"id": corpus_id, "name": name})
        return corpus_id

    def list_corpora(self, corpus_id: str | None = None) -> list[dict]:
        self._maybe_raise("list_corpora")
        if corpus_id:
            return [c for c in self.corpora if c.get("id") == corpus_id]
        return self.corpora

    def create_corpus(self, corpus_id: str, name: str) -> dict:
        self._maybe_raise("create_corpus")
        created = {"id": corpus_id, "name": name}
        self.corpora.append(created)
        self.last_created_corpus = created
        return created

    def delete_corpus(self, corpus_id: str) -> None:
        self._maybe_raise("delete_corpus")
        self.corpora = [c for c in self.corpora if c.get("id") != corpus_id]

    def upload_files(self, corpus_id, files) -> list[dict]:
        self._maybe_raise("upload_files")
        for f in files:
            self.uploaded_files.append(f.filename)
        return self.upload_results

    def list_documents(self, corpus_id, page_size: int = 50) -> list[dict]:
        self._maybe_raise("list_documents")
        return self.documents

    def delete_document(self, corpus_id, document_id) -> None:
        self._maybe_raise("delete_document")
        self.documents = [d for d in self.documents if d["id"] != document_id]

    # ---- Codebooks / themes -------------------------------------------------

    def delete_codebook(self, codebook_id: str) -> None:
        self._maybe_raise("delete_codebook")
        self.codebooks = [cb for cb in self.codebooks if cb.get("id") != codebook_id]

    def list_codebooks(self, corpus_id: str | None = None) -> list[dict]:
        self._maybe_raise("list_codebooks")
        if not corpus_id:
            return self.codebooks
        scoped = []
        for cb in self.codebooks:
            cb_corpus = cb.get("corpus_id")
            if cb_corpus is None or cb_corpus == corpus_id:
                scoped.append(cb)
        return scoped

    def get_codebook(self, codebook_id: str) -> dict:
        self._maybe_raise("get_codebook")
        for cb in self.codebooks:
            if cb.get("id") == codebook_id:
                return cb
        from web.services.backend_client import BackendNotFoundError
        raise BackendNotFoundError(user_message="Codebook not found.")

    def get_theme_frequencies(self, codebook_id: str) -> list[dict]:
        self._maybe_raise("get_theme_frequencies")
        return self.theme_frequencies

    def get_theme_tree(self, codebook_id: str) -> list[dict]:
        self._maybe_raise("get_theme_tree")
        return self.theme_tree

    def create_codebook(self, *, corpus_id: str, name: str, themes: list[dict]) -> dict:
        self._maybe_raise("create_codebook")
        self.last_create_codebook_request = {
            "corpus_id": corpus_id, "name": name, "themes": themes,
        }
        return {"id": "cb-new", "name": name, "corpus_id": corpus_id, "themes": themes}

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

    def list_demographic_rows(self, corpus_id, file_id, page=1, page_size=100) -> dict:
        self._maybe_raise("list_demographic_rows")
        total = len(self.demographic_rows)
        return {
            "items": self.demographic_rows,
            "meta": {"page": page, "pages": max(1, (total + page_size - 1) // page_size), "total": total}
        }

    def get_demographic_link_summary(self, corpus_id) -> dict:
        self._maybe_raise("get_demographic_link_summary")
        return self.demographic_link_summary

    def link_transcript(self, corpus_id, document_id, demographic_row_id) -> dict:
        self._maybe_raise("link_transcript")
        self.last_link_request = {
            "corpus_id": corpus_id,
            "document_id": document_id,
            "demographic_row_id": demographic_row_id,
        }
        return self.demographic_link_summary

    def unlink_transcript(self, corpus_id, document_id) -> dict:
        self._maybe_raise("unlink_transcript")
        self.last_unlink_request = {
            "corpus_id": corpus_id,
            "document_id": document_id,
        }
        return self.demographic_link_summary

    def delete_demographic_file(self, corpus_id, file_id) -> None:
        self._maybe_raise("delete_demographic_file")
        self.demographic_files = [
            f for f in self.demographic_files if f.get("id") != file_id
        ]

    # ---- Codebook generation jobs -------------------------------------------

    def create_generation_job(
        self,
        codebook_name: str,
        corpus_id: str,
        transcript_document_ids: list[str] | None = None,
        research_query: str | None = None,
        researcher_topics: str | None = None,
    ) -> dict:
        self._maybe_raise("create_generation_job")
        self.last_generation_job_request = {
            "codebook_name": codebook_name,
            "corpus_id": corpus_id,
            "transcript_document_ids": transcript_document_ids,
            "research_query": research_query,
            "researcher_topics": researcher_topics,
        }
        job_id = f"job-{len(self.generation_jobs) + 1}"
        job = {
            "id": job_id,
            "status": "queued",
            "codebook_name": codebook_name,
            "corpus_id": corpus_id,
            "transcript_document_ids": transcript_document_ids or [],
            "cancel_requested": False,
            "codebook_id": None,
            "passages_total": 0,
            "passages_done": 0,
        }
        self.generation_jobs[job_id] = job
        return job

    def list_generation_jobs(
        self, corpus_id: str, statuses: list[str] | None = None
    ) -> list[dict]:
        self._maybe_raise("list_generation_jobs")
        result = []
        for job in self.generation_jobs.values():
            job_corpus = job.get("corpus_id")
            if job_corpus is not None and job_corpus != corpus_id:
                continue
            if statuses and job.get("status") not in statuses:
                continue
            result.append(job)
        return result

    def get_generation_job(self, job_id: str) -> dict:
        self._maybe_raise("get_generation_job")
        return self.generation_jobs[job_id]

    def cancel_generation_job(self, job_id: str) -> dict:
        self._maybe_raise("cancel_generation_job")
        job = self.generation_jobs[job_id]
        job["cancel_requested"] = True
        job["status"] = "cancelled"
        return job

    # ---- Analysis Jobs ------------------------------------------------------
    
    def trigger_analysis(self, corpus_id: str, codebook_id: str) -> dict:
        self._maybe_raise("trigger_analysis")
        import uuid
        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "status": "queued",
            "corpus_id": corpus_id,
            "codebook_id": codebook_id,
            "passages_total": 5,
            "passages_done": 0,
        }
        if not hasattr(self, "analysis_jobs"):
            self.analysis_jobs = {}
        self.analysis_jobs[job_id] = job
        return job
        
    def get_analysis_job(self, job_id: str) -> dict:
        self._maybe_raise("get_analysis_job")
        if not hasattr(self, "analysis_jobs") or job_id not in self.analysis_jobs:
            return {"id": job_id, "status": "succeeded", "passages_total": 5, "passages_done": 5}
        return self.analysis_jobs[job_id]

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
    monkeypatch.setattr("web.controllers.analysis._backend", lambda: fake)
    return fake


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
def client(app):
    return app.test_client()
