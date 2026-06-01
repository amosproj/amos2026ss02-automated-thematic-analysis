"""HTTP client for the FastAPI backend.

Network and HTTP errors are categorised into typed BackendError subclasses,
each carrying a user-friendly default message and a log level. Controllers
catch BackendError (the base class) and surface `exc.user_message` to users.
"""

import json
import logging
import time
from typing import Iterable

import httpx
from flask import current_app, has_app_context
from werkzeug.datastructures import FileStorage


def get_backend_client() -> "BackendClient":
    """Return the shared BackendClient instance attached to the current Flask app.

    Centralised so controllers don't each redefine a `_backend()` helper —
    if we ever change where the client lives (e.g. move it out of
    `app.extensions`), there's exactly one place to update.
    """
    return current_app.extensions["backend_client"]


# Exception taxonomy


class BackendError(Exception):
    """Base class for all backend HTTP / network failures.

    Controllers catch this (or a subclass) and render `exc.user_message`
    to the user via `flash`. Subclasses override `default_user_message`
    and `log_level` for category-specific behaviour.
    """

    default_user_message: str = "Something went wrong. Please try again."
    log_level: str = "error"

    def __init__(
        self,
        user_message: str | None = None,
        *,
        source_exc: Exception | None = None,
        status_code: int | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(user_message or self.default_user_message)
        self.user_message = user_message or self.default_user_message
        self.source_exc = source_exc
        self.status_code = status_code
        self.path = path


class BackendUnavailableError(BackendError):
    """The backend is unreachable (connect refused, DNS failure, timeout).

    Transient — usually resolved by retrying or waiting for a deploy to finish.
    """

    default_user_message = (
        "We can't reach the analysis service right now. Please try again in a moment."
    )
    log_level = "warning"


class BackendNotFoundError(BackendError):
    """The backend returned 404 — the requested resource doesn't exist."""

    default_user_message = (
        "The requested item couldn't be found. It may have been deleted."
    )
    log_level = "info"


class BackendValidationError(BackendError):
    """The backend returned 422 — payload failed Pydantic validation.

    Parses FastAPI's structured detail list to extract human-readable
    per-field messages.
    """

    default_user_message = "Some of the data you provided didn't pass validation."
    log_level = "info"


class BackendServerError(BackendError):
    """The backend returned 5xx — internal server error on the backend side."""

    default_user_message = (
        "The analysis service had a problem. The team has been notified."
    )
    log_level = "error"


# Client

class BackendClient:
    def __init__(self, base_url: str, timeout: float = 60.0) -> None:
        # Reused across requests; closed via `close()` on app teardown.
        self._client = httpx.Client(base_url=base_url, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    # ---- Corpora ------------------------------------------------------------

    def list_corpora(self, project_id: str) -> list[dict]:
        return self._get("/ingestion/corpora", params={"project_id": project_id}, sub_key="items")

    def create_corpus(self, project_id: str, name: str) -> dict:
        return self._post("/ingestion/corpora", json={"project_id": project_id, "name": name})

    def ensure_corpus(self, project_id: str, name: str) -> str:
        """Return the first corpus_id for `project_id`, creating one if none exists.

        Re-checks the backend on every call — no in-memory cache. The id is
        carried in the URL after the initial landing redirect, so this is only
        invoked once per session entry."""
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
        path = f"/ingestion/corpora/{corpus_id}/upload"
        started_at = time.monotonic()
        try:
            r = self._client.post(path, files=multipart)
            r.raise_for_status()
            return self._unwrap(r, sub_key="results")
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "POST", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "POST", started_at)

    def list_documents(self, corpus_id: str, page_size: int = 50) -> list[dict]:
        return self._get(
            f"/ingestion/corpora/{corpus_id}/documents",
            params={"page_size": page_size},
            sub_key="items",
        )

    # ---- Codebooks ----------------------------------------------------------

    def list_codebooks(self, corpus_id: str | None = None) -> list[dict]:
        params = {"corpus_id": corpus_id} if corpus_id else None
        return self._get("/codebooks/", params=params)

    def get_theme_frequencies(self, codebook_id: str) -> list[dict]:
        return self._get(f"/codebooks/{codebook_id}/themes")

    def get_theme_tree(self, codebook_id: str) -> list[dict]:
        return self._get(f"/codebooks/{codebook_id}/themes/tree")

    # ---- Demographic --------------------------------------------------------

    def upload_demographic(
        self, corpus_id: str, file: FileStorage, name: str | None = None,
    ) -> dict:
        """POST a demographic CSV to the backend; returns preview + import_id."""
        path = f"/demographic/{corpus_id}/upload"
        multipart = [
            ("file", (file.filename, file.stream, file.mimetype or "text/csv")),
        ]
        data = {}
        if name:
            data["name"] = name
        started_at = time.monotonic()
        try:
            r = self._client.post(path, files=multipart, data=data)
            r.raise_for_status()
            return self._unwrap(r)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "POST", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "POST", started_at)

    def confirm_demographic(
        self, corpus_id: str, import_id: str, confirm: bool,
    ) -> dict:
        """Confirm or cancel a pending demographic upload."""
        path = f"/demographic/{corpus_id}/confirm"
        return self._post(
            path,
            params={"import_id": import_id, "confirm": str(confirm).lower()},
        )

    def list_demographic_files(
        self, corpus_id: str, page_size: int = 200,
    ) -> list[dict]:
        """List confirmed demographic imports for one corpus."""
        return self._get(
            f"/demographic/{corpus_id}/files",
            params={"page_size": page_size},
            sub_key="items",
        )

    def list_demographic_rows(
        self,
        corpus_id: str,
        file_id: str,
        page: int = 1,
        page_size: int = 100,
    ) -> dict:
        """List demographic rows for a specific file, returning items and pagination meta."""
        return self._get(
            f"/demographic/{corpus_id}/rows",
            params={
                "demographic_file_id": file_id,
                "page": page,
                "page_size": page_size,
            },
        )

    def get_demographic_link_summary(self, corpus_id: str) -> dict:
        """Get transcript ↔ demographic linking status."""
        return self._get(f"/demographic/{corpus_id}/link-summary")

    # ---- Helpers ------------------------------------------------------------

    def _unwrap(self, response: httpx.Response, *, sub_key: str | None = None):
        """Peel the FastAPI envelope `{success, data, error, meta}`.

        Returns `response.json()["data"]` by default, or
        `response.json()["data"][sub_key]` when sub_key is given — used for
        paginated responses (`items`) and the multipart upload (`results`).
        Centralising this means the envelope shape lives in exactly one place;
        if the backend ever changes it, only this helper needs updating.
        """
        payload = response.json()["data"]
        return payload if sub_key is None else payload[sub_key]

    def _get(self, path: str, *, sub_key: str | None = None, **kwargs):
        started_at = time.monotonic()
        try:
            r = self._client.get(path, **kwargs)
            r.raise_for_status()
            return self._unwrap(r, sub_key=sub_key)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "GET", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "GET", started_at)

    def _post(self, path: str, *, sub_key: str | None = None, **kwargs):
        started_at = time.monotonic()
        try:
            r = self._client.post(path, **kwargs)
            r.raise_for_status()
            return self._unwrap(r, sub_key=sub_key)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "POST", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "POST", started_at)

    def _handle_exc(
        self,
        exc: Exception,
        path: str,
        method: str,
        started_at: float,
    ) -> None:
        """Categorise an exception, log it, and re-raise as the right BackendError subclass.

        Never returns — always raises. The return-type annotation is `None`
        so type checkers understand the flow.
        """
        duration = time.monotonic() - started_at
        status_code: int | None = None
        error: BackendError

        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            if status_code == 404:
                error = BackendNotFoundError(
                    source_exc=exc, status_code=status_code, path=path
                )
            elif status_code == 422:
                error = BackendValidationError(
                    user_message=_parse_validation_detail(exc.response),
                    source_exc=exc,
                    status_code=status_code,
                    path=path,
                )
            elif 500 <= status_code < 600:
                error = BackendServerError(
                    source_exc=exc, status_code=status_code, path=path
                )
            else:
                error = BackendError(
                    source_exc=exc, status_code=status_code, path=path
                )
        elif isinstance(
            exc,
            (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, httpx.NetworkError),
        ):
            error = BackendUnavailableError(source_exc=exc, path=path)
        else:
            # JSONDecodeError, KeyError, other httpx.HTTPError subclasses
            error = BackendError(source_exc=exc, path=path)

        _log_backend_error(error, method, path, duration)
        raise error from exc


# Module-level helpers


def _parse_validation_detail(response: httpx.Response) -> str | None:
    """Extract a human-readable message from a FastAPI 422 response.

    FastAPI returns: {"detail": [{"loc": ["body", "name"], "msg": "..."}, ...]}
    Returns None if the body is missing or malformed — caller falls back to default.
    """
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(body, dict):
        return None

    detail = body.get("detail")
    if isinstance(detail, str) and detail.strip():
        return detail

    if not isinstance(detail, list):
        # Our backend envelope for handled 422s is:
        # {"success": false, "error": "...", "meta": {"detail": "..."}}
        meta = body.get("meta")
        if isinstance(meta, dict):
            meta_detail = meta.get("detail")
            if isinstance(meta_detail, str) and meta_detail.strip():
                return meta_detail
        return None

    messages: list[str] = []
    for item in detail:
        if not isinstance(item, dict):
            continue
        loc = item.get("loc") or []
        msg = item.get("msg")
        if not msg:
            continue
        # Skip the leading "body" / "query" / "path" location segment for readability.
        field_path = ".".join(str(p) for p in loc if p not in ("body", "query", "path"))
        messages.append(f"{field_path}: {msg}" if field_path else str(msg))
    return "; ".join(messages) if messages else None


def _log_backend_error(
    error: BackendError, method: str, path: str, duration: float
) -> None:
    """Emit a single structured log line at the level defined on the error class.

    Falls back to the stdlib root logger if no Flask app context exists
    (e.g. in unit tests that exercise the client directly).
    """
    logger = current_app.logger if has_app_context() else logging.getLogger("backend_client")
    level = getattr(logging, error.log_level.upper(), logging.ERROR)
    status_part = f" status={error.status_code}" if error.status_code else ""
    source_part = (
        f" {type(error.source_exc).__name__}: {error.source_exc}"
        if error.source_exc
        else ""
    )
    logger.log(
        level,
        "backend_client %s %s -> %s (%.2fs)%s%s",
        method,
        path,
        type(error).__name__,
        duration,
        status_part,
        source_part,
    )
