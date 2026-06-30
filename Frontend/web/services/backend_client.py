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


class BackendConflictError(BackendError):
    """The backend returned 409 — the request conflicts with live state."""

    default_user_message = (
        "This action conflicts with the current analysis state. Please review and try again."
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

    def list_corpora(self, corpus_id: str | None = None) -> list[dict]:
        params = {"corpus_id": corpus_id} if corpus_id else {}
        return self._get("/ingestion/corpora", params=params, sub_key="items")

    def create_corpus(self, corpus_id: str, name: str) -> dict:
        return self._post("/ingestion/corpora", json={"corpus_id": corpus_id, "name": name})

    def ensure_corpus(self, corpus_id: str, name: str) -> str:
        """Return the first corpus_id for `corpus_id`, creating one if none exists.

        Re-checks the backend on every call — no in-memory cache. The id is
        carried in the URL after the initial landing redirect, so this is only
        invoked once per session entry."""
        existing = self.list_corpora(corpus_id)
        if existing:
            return existing[0]["id"]
        return self.create_corpus(corpus_id, name)["id"]

    def delete_corpus(self, corpus_id: str, *, force: bool = False) -> None:
        """Delete a corpus and all its associated data."""
        path = f"/ingestion/corpora/{corpus_id}"
        params = {"force": "true"} if force else None
        started_at = time.monotonic()
        try:
            r = self._client.delete(path, params=params)
            r.raise_for_status()
            return self._unwrap(r)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "DELETE", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "DELETE", started_at)

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

    def get_document_content(self, corpus_id: str, document_id: str) -> dict:
        """Fetch a single document including its full text content."""
        return self._get(f"/ingestion/corpora/{corpus_id}/documents/{document_id}")

    def delete_document(self, corpus_id: str, document_id: str, *, force: bool = False) -> None:
        """Delete a document from a corpus."""
        path = f"/ingestion/corpora/{corpus_id}/documents/{document_id}"
        params = {"force": "true"} if force else None
        started_at = time.monotonic()
        try:
            r = self._client.delete(path, params=params)
            r.raise_for_status()
            return self._unwrap(r)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "DELETE", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "DELETE", started_at)

    # ---- Codebooks ----------------------------------------------------------


    def get_theme_frequencies(
        self, codebook_id: str, application_run_id: str | None = None
    ) -> list[dict]:
        params = {"application_run_id": application_run_id} if application_run_id else None
        return self._get(f"/codebooks/{codebook_id}/themes", params=params)

    def get_theme_tree(self, codebook_id: str) -> list[dict]:
        return self._get(f"/codebooks/{codebook_id}/themes/tree")

    def get_theme_quotes(
        self,
        codebook_id: str,
        theme_id: str,
        page: int = 1,
        page_size: int = 20,
        application_run_id: str | None = None,
    ) -> dict:
        params = {"page": page, "page_size": page_size}
        if application_run_id:
            params["application_run_id"] = application_run_id
        return self._get(
            f"/codebooks/{codebook_id}/themes/{theme_id}/quotes",
            params=params,
        )

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

    def link_transcript(
        self, corpus_id: str, document_id: str, demographic_row_id: str,
    ) -> dict:
        """Manually link (or reassign) a transcript to a demographic row."""
        return self._put(
            f"/demographic/{corpus_id}/documents/{document_id}/link",
            json={"demographic_row_id": demographic_row_id},
        )

    def unlink_transcript(self, corpus_id: str, document_id: str) -> dict:
        """Remove the demographic link from a transcript."""
        return self._delete(
            f"/demographic/{corpus_id}/documents/{document_id}/link"
        )

    def delete_demographic_file(self, corpus_id: str, file_id: str) -> None:
        """Delete a demographic file from the backend."""
        path = f"/demographic/{corpus_id}/files/{file_id}"
        started_at = time.monotonic()
        try:
            r = self._client.delete(path)
            r.raise_for_status()
            return self._unwrap(r)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "DELETE", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "DELETE", started_at)

    # ---- Analysis Jobs ------------------------------------------------------

    def trigger_analysis(
        self,
        corpus_id: str,
        codebook_id: str,
        name: str | None = None,
        custom_id: str | None = None,
        transcript_document_ids: list[str] | None = None,
    ) -> dict:
        payload: dict = {}
        if name:
            payload["name"] = name
        if custom_id:
            payload["custom_id"] = custom_id
        if transcript_document_ids:
            payload["transcript_document_ids"] = transcript_document_ids
            
        return self._post(f"/codebooks/{codebook_id}/apply-jobs", json=payload)

    def get_analysis_job(self, job_id: str) -> dict:
        return self._get(f"/codebooks/apply-jobs/{job_id}")

    def cancel_analysis_job(self, job_id: str) -> dict:
        return self._post(f"/codebooks/apply-jobs/{job_id}/cancel")

    def list_codebook_application_runs(self, codebook_id: str) -> list[dict]:
        result = self._get(f"/codebooks/{codebook_id}/application-runs")
        if isinstance(result, list):
            return result
        return result.get("items", result) if isinstance(result, dict) else []

    def delete_codebook_application_run(self, run_id: str) -> None:
        """Hard-delete an analysis run and its coded results."""
        self._delete(f"/codebook-application-runs/{run_id}")

    def fetch_run_export_csv(self, run_id: str, export_format: str) -> bytes:
        """Fetch a run's CSV export as raw bytes.

        The export endpoint returns raw CSV, not the JSON envelope, so this
        bypasses _get / _unwrap.
        """
        path = f"/codebook-application-runs/{run_id}/export"
        started_at = time.monotonic()
        try:
            r = self._client.get(path, params={"format": export_format})
            r.raise_for_status()
            return r.content
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "GET", started_at)

    # ---- Codebook Upload & Parsing ------------------------------------------

    def parse_csv_preview(self, file: FileStorage) -> list[dict]:
        """Send a CSV file to the backend parser to get a theme preview list."""
        multipart = {
            "file": (file.filename, file.stream, file.mimetype or "text/csv")
        }
        started_at = time.monotonic()
        try:
            r = self._client.post("/codebooks/parse-csv", files=multipart)
            r.raise_for_status()
            return self._unwrap(r)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, "/codebooks/parse-csv", "POST", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, "/codebooks/parse-csv", "POST", started_at)

    def create_codebook(self, corpus_id: str, name: str, themes: list[dict]) -> dict:
        """Persist a new codebook and its themes in the backend database."""
        return self._post(
            "/codebooks/",
            json={"corpus_id": corpus_id, "name": name, "nodes": themes},
        )

    def get_codebook(self, codebook_id: str) -> dict:
        """Fetch details of a codebook by its unique UUID."""
        return self._get(f"/codebooks/{codebook_id}")

    def list_codebooks(self, corpus_id: str | None = None) -> list[dict]:
        """Return all persisted codebooks for a given corpus, ordered by descending version."""
        params = {"corpus_id": corpus_id} if corpus_id else None
        result = self._get("/codebooks/", params=params)
        # The envelope `data` field is a list directly for this endpoint
        if isinstance(result, list):
            return result
        return result.get("items", result) if isinstance(result, dict) else []

    def delete_codebook(self, codebook_id: str, *, force: bool = False) -> None:
        """Delete a codebook and all its associated themes/codes via cascade."""
        params = {"force": "true"} if force else None
        self._delete(f"/codebooks/{codebook_id}", params=params)

    # ---- Codebook generation jobs -------------------------------------------

    def create_generation_job(
        self,
        codebook_name: str,
        corpus_id: str,
        research_query: str | None = None,
        researcher_topics: str | None = None,
        transcript_document_ids: list[str] | None = None,
    ) -> dict:
        payload: dict = {"codebook_name": codebook_name, "corpus_id": corpus_id}
        if research_query:
            payload["research_query"] = research_query
        if researcher_topics:
            payload["researcher_topics"] = researcher_topics
        if transcript_document_ids:
            payload["transcript_document_ids"] = transcript_document_ids
        return self._post("/codebooks/generate-jobs", json=payload)

    def list_generation_jobs(
        self,
        corpus_id: str,
        statuses: list[str] | None = None,
    ) -> list[dict]:
        """Return generation jobs for a corpus, optionally filtered by status.

        Used to render in-progress runs in the codebook list as a server-side
        source of truth (visible in any browser/session), independent of the
        client-side localStorage tracker.
        """
        params: dict = {"corpus_id": corpus_id}
        if statuses:
            params["status"] = ",".join(statuses)
        result = self._get("/codebooks/generate-jobs", params=params)
        if isinstance(result, list):
            return result
        return result.get("items", result) if isinstance(result, dict) else []

    def get_generation_job(self, job_id: str) -> dict:
        return self._get(f"/codebooks/generate-jobs/{job_id}")

    def cancel_generation_job(self, job_id: str) -> dict:
        return self._post(f"/codebooks/generate-jobs/{job_id}/cancel")

    # ---- Settings -----------------------------------------------------------

    def get_llm_provider(self) -> dict:
        """Return the active LLM provider plus available options and default.

        Shape: {"active": str, "default": str, "available": [{id, label,
        description, has_api_key}]}.
        """
        return self._get("/settings/llm-provider")

    def set_llm_provider(self, provider: str) -> dict:
        """Persist the active LLM provider; returns the updated provider state."""
        return self._put("/settings/llm-provider", json={"provider": provider})

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

    def _delete(self, path: str, *, sub_key: str | None = None, **kwargs):
        started_at = time.monotonic()
        try:
            r = self._client.delete(path, **kwargs)
            r.raise_for_status()
            return self._unwrap(r, sub_key=sub_key)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "DELETE", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "DELETE", started_at)

    def _put(self, path: str, *, sub_key: str | None = None, **kwargs):
        started_at = time.monotonic()
        try:
            r = self._client.put(path, **kwargs)
            r.raise_for_status()
            return self._unwrap(r, sub_key=sub_key)
        except httpx.HTTPError as exc:
            self._handle_exc(exc, path, "PUT", started_at)
        except (json.JSONDecodeError, KeyError) as exc:
            self._handle_exc(exc, path, "PUT", started_at)

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
            elif status_code == 409:
                error = BackendConflictError(
                    user_message=_parse_validation_detail(exc.response),
                    source_exc=exc,
                    status_code=status_code,
                    path=path,
                )
            elif status_code == 422:
                if has_app_context():
                    current_app.logger.error("422 Validation Error Payload: %s", exc.response.text)
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
        # Our backend envelope for handled 4xx errors is one of:
        #   {"success": false, "error": "...", "meta": {"detail": "..."}}
        #   {"success": false, "error": "...", "meta": null}   (e.g. validation
        #     errors raised in services, where the message lives in `error`)
        # Prefer the more specific meta.detail, then fall back to the top-level
        # `error` so service-raised messages (e.g. "<provider> has no API key
        # configured") reach the user instead of the generic default.
        meta = body.get("meta")
        if isinstance(meta, dict):
            meta_detail = meta.get("detail")
            if isinstance(meta_detail, str) and meta_detail.strip():
                return meta_detail
        error = body.get("error")
        if isinstance(error, str) and error.strip():
            return error
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
