"""HTTP client for the FastAPI backend. Network/HTTP errors → BackendError."""

from typing import Iterable

import httpx
from werkzeug.datastructures import FileStorage


class BackendError(Exception):
    """Wraps httpx errors and non-2xx responses; controllers render `str(exc)`."""


class BackendClient:
    def __init__(self, base_url: str, timeout: float = 60.0) -> None:
        # Reused across requests; closed via `close()` on app teardown.
        self._client = httpx.Client(base_url=base_url, timeout=timeout)
        self._corpus_id_by_project: dict[str, str] = {}

    def close(self) -> None:
        self._client.close()

    # ---- Corpora ------------------------------------------------------------

    def list_corpora(self, project_id: str) -> list[dict]:
        return self._get("/ingestion/corpora", params={"project_id": project_id})["items"]

    def create_corpus(self, project_id: str, name: str) -> dict:
        return self._post("/ingestion/corpora", json={"project_id": project_id, "name": name})

    def ensure_corpus(self, project_id: str, name: str) -> str:
        """Return the first corpus_id for `project_id`, creating one if none exists. Memoised."""
        if project_id in self._corpus_id_by_project:
            return self._corpus_id_by_project[project_id]
        existing = self.list_corpora(project_id)
        corpus_id = existing[0]["id"] if existing else self.create_corpus(project_id, name)["id"]
        self._corpus_id_by_project[project_id] = corpus_id
        return corpus_id

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
            r = self._client.post(f"/ingestion/corpora/{corpus_id}/upload", files=multipart)
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
            r = self._client.get(path, **kwargs)
            r.raise_for_status()
            return r.json()["data"]
        except httpx.HTTPError as exc:
            raise BackendError(f"Backend GET {path} failed: {exc}") from exc

    def _post(self, path: str, **kwargs) -> dict:
        try:
            r = self._client.post(path, **kwargs)
            r.raise_for_status()
            return r.json()["data"]
        except httpx.HTTPError as exc:
            raise BackendError(f"Backend POST {path} failed: {exc}") from exc
