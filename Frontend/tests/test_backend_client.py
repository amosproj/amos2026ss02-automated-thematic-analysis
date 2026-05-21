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


# User messages never leak raw exception text


def test_user_message_does_not_leak_raw_exception_text():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("ConnectionRefusedError(111)", request=request)

    client = _client_with_handler(handler)
    with pytest.raises(BackendUnavailableError) as exc_info:
        client.list_codebooks()
    assert "ConnectionRefusedError" not in exc_info.value.user_message
    assert "111" not in exc_info.value.user_message
