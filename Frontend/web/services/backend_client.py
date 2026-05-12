"""HTTP client for the FastAPI backend. Network/HTTP errors → BackendError."""

from typing import Iterable

import httpx
from werkzeug.datastructures import FileStorage


class BackendError(Exception):
    """Wraps httpx errors and non-2xx responses; controllers render `str(exc)`."""


class BackendClient:
    def __init__(self, base_url: str, timeout: float = 60.0) -> None:
        self._base_url = base_url
        self._timeout = timeout

    def _client(self) -> httpx.Client:
        return httpx.Client(base_url=self._base_url, timeout=self._timeout)

    # ---- Corpora ------------------------------------------------------------

    def list_corpora(self, project_id: str) -> list[dict]:
        return self._get("/ingestion/corpora", params={"project_id": project_id})["items"]

    def create_corpus(self, project_id: str, name: str) -> dict:
        return self._post("/ingestion/corpora", json={"project_id": project_id, "name": name})

    def ensure_corpus(self, project_id: str, name: str) -> str:
        """Return the first corpus_id for `project_id`, creating one if none exists."""
        existing = self.list_corpora(project_id)
        if existing:
            return existing[0]["id"]
        return self.create_corpus(project_id, name)["id"]

    # ---- Documents ----------------------------------------------------------

    def upload_files(
        self, corpus_id: str, files: Iterable[FileStorage]
    ) -> list[dict]:
        """POST files to the backend's multipart endpoint; return per-file results."""
        multipart = [
            ("files", (f.filename, f.stream, f.mimetype or "application/octet-stream"))
            for f in files
        ]
        try:
            with self._client() as c:
                r = c.post(f"/ingestion/corpora/{corpus_id}/upload", files=multipart)
                r.raise_for_status()
                return r.json()["data"]["results"]
        except httpx.HTTPError as exc:
            raise BackendError(f"Backend upload failed: {exc}") from exc

    def list_documents(self, corpus_id: str, page_size: int = 50) -> list[dict]:
        return self._get(
            f"/ingestion/corpora/{corpus_id}/documents",
            params={"page_size": page_size},
        )["items"]

    # ---- Helpers ------------------------------------------------------------

    def _get(self, path: str, **kwargs) -> dict:
        try:
            with self._client() as c:
                r = c.get(path, **kwargs)
                r.raise_for_status()
                return r.json()["data"]
        except httpx.HTTPError as exc:
            raise BackendError(f"Backend GET {path} failed: {exc}") from exc

    def _post(self, path: str, **kwargs) -> dict:
        try:
            with self._client() as c:
                r = c.post(path, **kwargs)
                r.raise_for_status()
                return r.json()["data"]
        except httpx.HTTPError as exc:
            raise BackendError(f"Backend POST {path} failed: {exc}") from exc
