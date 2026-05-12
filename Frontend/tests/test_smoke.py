"""Smoke tests for the Frontend scaffolding. Verifies the app factory, every
registered route, and the upload form's hint rendering."""


def test_app_factory_returns_flask_app(app):
    assert app is not None
    expected_routes = {"/", "/health", "/transcripts/", "/transcripts/upload"}
    actual_routes = {r.rule for r in app.url_map.iter_rules()}
    assert expected_routes <= actual_routes


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}


def test_index_renders(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Automated Thematic Analysis" in resp.data


def test_upload_form_renders_with_config(client):
    """Form must reflect the configured size limit and accepted extensions"""
    resp = client.get("/transcripts/upload")
    assert resp.status_code == 200
    assert b"Upload Interview Transcripts" in resp.data
    assert b"10 MB" in resp.data
    for ext in (b".txt", b".docx", b".pdf", b".jsonl"):
        assert ext in resp.data
