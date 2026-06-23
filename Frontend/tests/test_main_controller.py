"""Tests for the Home page LLM provider dropdown (web.controllers.main)."""
from __future__ import annotations


def test_home_renders_provider_dropdown(client, fake_backend) -> None:
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.data.decode()
    assert "LLM Provider" in body
    # Both providers from the backend appear as options.
    assert "FAU NHR" in body
    assert "Academic Cloud" in body
    # The active provider is preselected.
    assert 'value="FAU"' in body and "selected" in body


def test_home_degrades_when_backend_down(client, fake_backend) -> None:
    fake_backend.raise_on = "get_llm_provider"
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.data.decode()
    # Card still renders (fallback state) and the control is disabled.
    assert "LLM Provider" in body
    assert "unreachable" in body.lower()


def test_set_provider_persists_and_flashes(client, fake_backend) -> None:
    resp = client.post(
        "/settings/llm-provider",
        data={"provider": "ACADEMIC"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert fake_backend.last_set_provider == "ACADEMIC"
    assert "Academic Cloud" in resp.data.decode()


def test_set_provider_blank_is_rejected(client, fake_backend) -> None:
    resp = client.post(
        "/settings/llm-provider",
        data={"provider": ""},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    # No backend call was made for a blank selection.
    assert fake_backend.last_set_provider is None
    assert "choose an llm provider" in resp.data.decode().lower()


def test_set_provider_surfaces_backend_error(client, fake_backend) -> None:
    fake_backend.raise_on = "set_llm_provider"
    resp = client.post(
        "/settings/llm-provider",
        data={"provider": "ACADEMIC"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    # The friendly BackendError message is flashed.
    assert "simulated set_llm_provider failure" in resp.data.decode()
