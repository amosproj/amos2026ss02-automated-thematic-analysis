"""Tests for the Home and Help pages (web.controllers.main)."""
from __future__ import annotations


def test_home_renders_provider_dropdown(client, fake_backend) -> None:
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.data.decode()
    assert "LLM Provider" in body
    # All providers from the backend appear as options.
    assert "FAU NHR" in body
    assert "Academic Cloud" in body
    assert "OpenRouter (OpenAI GPT-5.6)" in body
    # The active provider is preselected.
    assert 'value="FAU"' in body and "selected" in body


def test_home_renders_generation_algorithm_dropdown(client, fake_backend) -> None:
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.data.decode()
    assert "Codebook Generation Algorithm" in body
    assert "Traceable Analysis" in body
    assert "Keyword Frequency (Demo)" in body
    assert 'value="traceable_analysis"' in body
    assert "Add a custom generation algorithm" in body


def test_help_describes_openrouter_models_and_selected_algorithms(client) -> None:
    resp = client.get("/help")
    assert resp.status_code == 200
    body = resp.data.decode()

    assert "How do I add further models through OpenRouter?" in body
    assert "openai/gpt-5.6-sol" in body
    assert "How do I add a custom codebook-generation algorithm?" in body
    assert "Why does Traceable Analysis hold out some transcripts?" in body
    assert "ignoring capitalization and surrounding whitespace" in body

    assert "Can I use a commercial embedding provider such as OpenAI?" not in body
    assert "Adding an External LLM Provider" not in body


def test_home_degrades_when_backend_down(client, fake_backend) -> None:
    fake_backend.raise_on = "get_llm_provider"
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.data.decode()
    # Card still renders (fallback state) and the control is disabled.
    assert "LLM Provider" in body
    assert "unreachable" in body.lower()


def test_algorithm_dropdown_degrades_when_backend_down(client, fake_backend) -> None:
    fake_backend.raise_on = "get_generation_algorithm"
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.data.decode()
    assert "Codebook Generation Algorithm" in body
    assert "algorithm can't be changed" in body.lower()


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


def test_set_generation_algorithm_persists_and_flashes(client, fake_backend) -> None:
    resp = client.post(
        "/settings/generation-algorithm",
        data={"algorithm": "keyword_frequency_example"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert fake_backend.last_set_algorithm == "keyword_frequency_example"
    assert "Generation algorithm set to Keyword Frequency (Demo)" in resp.data.decode()


def test_set_generation_algorithm_blank_is_rejected(client, fake_backend) -> None:
    resp = client.post(
        "/settings/generation-algorithm",
        data={"algorithm": ""},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert fake_backend.last_set_algorithm is None
    assert "choose a generation algorithm" in resp.data.decode().lower()


def test_set_generation_algorithm_surfaces_backend_error(client, fake_backend) -> None:
    fake_backend.raise_on = "set_generation_algorithm"
    resp = client.post(
        "/settings/generation-algorithm",
        data={"algorithm": "keyword_frequency_example"},
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert "simulated set_generation_algorithm failure" in resp.data.decode()
