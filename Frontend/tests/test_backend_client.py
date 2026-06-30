# test_backend_client.py — unit tests for BackendClient exception categorisation.
#
# Uses httpx.MockTransport to simulate every error category the client must
# distinguish (connect failure, timeout, 404, 422 with FastAPI detail, 5xx,
# malformed JSON). Verifies the right BackendError subclass is raised AND
# that the user-facing message is appropriate (no raw exception text leaking).
#
# These are pure unit tests — no Flask app, no Docker, no real backend.

import httpx
import pytest

from web.services.backend_client import (
    BackendClient,
    BackendError,
    BackendConflictError,
    BackendNotFoundError,
    BackendServerError,
    BackendUnavailableError,
    BackendValidationError,
)


def _client_with_handler(handler) -> BackendClient:
    """Build a BackendClient whose underlying httpx.Client routes through
    a MockTransport returning whatever the handler decides."""
    transport = httpx.MockTransport(handler)
    client = BackendClient("http://test-backend/api/v1")
    client._client = httpx.Client(transport=transport, base_url="http://test-backend/api/v1")
    return client


# Network / connection failures > BackendUnavailableError


def test_connect_error_maps_to_unavailable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client = _client_with_handler(handler)
    with pytest.raises(BackendUnavailableError) as exc_info:
        client.list_codebooks()
    assert "can't reach the analysis service" in exc_info.value.user_message


def test_read_timeout_maps_to_unavailable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timed out", request=request)

    client = _client_with_handler(handler)
    with pytest.raises(BackendUnavailableError):
        client.list_codebooks()


# HTTP status codes > typed subclasses


def test_status_404_maps_to_not_found():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "not found"})

    client = _client_with_handler(handler)
    with pytest.raises(BackendNotFoundError) as exc_info:
        client.list_codebooks()
    assert exc_info.value.status_code == 404
    assert "couldn't be found" in exc_info.value.user_message


def test_status_422_parses_fastapi_validation_detail():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            json={
                "detail": [
                    {"loc": ["body", "name"], "msg": "field required"},
                    {"loc": ["body", "themes"], "msg": "must contain at least 1 item"},
                ]
            },
        )

    client = _client_with_handler(handler)
    with pytest.raises(BackendValidationError) as exc_info:
        client.list_codebooks()
    msg = exc_info.value.user_message
    assert "name: field required" in msg
    assert "themes: must contain at least 1 item" in msg


def test_status_422_parses_response_envelope_meta_detail():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            json={
                "success": False,
                "error": "UnprocessableError",
                "meta": {"detail": "username already exists: 'user_a'"},
            },
        )

    client = _client_with_handler(handler)
    with pytest.raises(BackendValidationError) as exc_info:
        client.list_codebooks()
    assert "username already exists" in exc_info.value.user_message


def test_status_409_parses_response_envelope_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "success": False,
                "error": "Deleting this transcript would interrupt a running analysis.",
                "meta": None,
            },
        )

    client = _client_with_handler(handler)
    with pytest.raises(BackendConflictError) as exc_info:
        client.delete_document("corpus-1", "doc-1")
    assert "interrupt a running analysis" in exc_info.value.user_message


def test_status_500_maps_to_server_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"detail": "boom"})

    client = _client_with_handler(handler)
    with pytest.raises(BackendServerError) as exc_info:
        client.list_codebooks()
    assert exc_info.value.status_code == 500


def test_status_503_maps_to_server_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = _client_with_handler(handler)
    with pytest.raises(BackendServerError):
        client.list_codebooks()


# Malformed responses > generic BackendError


def test_malformed_json_maps_to_generic_backend_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json at all")

    client = _client_with_handler(handler)
    with pytest.raises(BackendError) as exc_info:
        client.list_codebooks()
    # Not the typed subclasses — exactly the base class.
    assert type(exc_info.value) is BackendError


# Codebook generation jobs > envelope unwrap + payload shaping


def test_create_generation_job_sends_payload_and_unwraps_envelope():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = request.read()
        return httpx.Response(
            202,
            json={
                "success": True,
                "data": {"id": "job-1", "status": "queued"},
                "error": None,
                "meta": None,
            },
        )

    client = _client_with_handler(handler)
    job = client.create_generation_job(
        codebook_name="My Codebook",
        corpus_id="11111111-1111-1111-1111-111111111111",
    )
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/codebooks/generate-jobs")
    body = captured["body"].decode()
    assert "My Codebook" in body
    assert "11111111-1111-1111-1111-111111111111" in body
    # transcript_document_ids omitted when empty/None
    assert "transcript_document_ids" not in body
    assert job == {"id": "job-1", "status": "queued"}


def test_create_generation_job_includes_transcript_ids_when_provided():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = request.read()
        return httpx.Response(
            202,
            json={"success": True, "data": {"id": "job-2"}, "error": None, "meta": None},
        )

    client = _client_with_handler(handler)
    client.create_generation_job(
        codebook_name="cb",
        corpus_id="cid",
        transcript_document_ids=["d1", "d2"],
    )
    body = captured["body"].decode()
    assert "transcript_document_ids" in body
    assert "d1" in body and "d2" in body


def test_get_generation_job_returns_unwrapped_data():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/codebooks/generate-jobs/job-42")
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {"id": "job-42", "status": "succeeded", "codebook_id": "cb-9"},
                "error": None,
                "meta": None,
            },
        )

    client = _client_with_handler(handler)
    job = client.get_generation_job("job-42")
    assert job["status"] == "succeeded"
    assert job["codebook_id"] == "cb-9"


def test_cancel_generation_job_posts_and_unwraps():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(
            202,
            json={
                "success": True,
                "data": {"id": "job-7", "status": "cancelled", "cancel_requested": True},
                "error": None,
                "meta": None,
            },
        )

    client = _client_with_handler(handler)
    job = client.cancel_generation_job("job-7")
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/codebooks/generate-jobs/job-7/cancel")
    assert job["status"] == "cancelled"


def test_cancel_analysis_job_posts_and_unwraps():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(
            202,
            json={
                "success": True,
                "data": {"id": "job-8", "status": "running", "cancel_requested": True},
                "error": None,
                "meta": None,
            },
        )

    client = _client_with_handler(handler)
    job = client.cancel_analysis_job("job-8")
    assert captured["method"] == "POST"
    assert captured["path"].endswith("/codebooks/apply-jobs/job-8/cancel")
    assert job["cancel_requested"] is True


# User messages never leak raw exception text


def test_user_message_does_not_leak_raw_exception_text():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("ConnectionRefusedError(111)", request=request)

    client = _client_with_handler(handler)
    with pytest.raises(BackendUnavailableError) as exc_info:
        client.list_codebooks()
    assert "ConnectionRefusedError" not in exc_info.value.user_message
    assert "111" not in exc_info.value.user_message


# Manual linking client methods (PUT/DELETE on the document link)


def test_link_transcript_issues_put_with_row_id():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["body"] = request.content.decode()
        return httpx.Response(
            200,
            json={"success": True, "data": {"matched": 1, "details": [], "demographic_rows": []},
                  "error": None, "meta": None},
        )

    client = _client_with_handler(handler)
    summary = client.link_transcript("corpus-1", "doc-1", "row-9")
    assert captured["method"] == "PUT"
    assert captured["path"].endswith("/demographic/corpus-1/documents/doc-1/link")
    assert "row-9" in captured["body"]
    assert summary["matched"] == 1


def test_unlink_transcript_issues_delete():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        return httpx.Response(
            200,
            json={"success": True, "data": {"matched": 0, "details": [], "demographic_rows": []},
                  "error": None, "meta": None},
        )

    client = _client_with_handler(handler)
    summary = client.unlink_transcript("corpus-1", "doc-1")
    assert captured["method"] == "DELETE"
    assert captured["path"].endswith("/demographic/corpus-1/documents/doc-1/link")
    assert summary["matched"] == 0


def test_link_transcript_422_maps_to_validation_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"detail": "row not in corpus"})

    client = _client_with_handler(handler)
    with pytest.raises(BackendValidationError):
        client.link_transcript("corpus-1", "doc-1", "bad-row")
