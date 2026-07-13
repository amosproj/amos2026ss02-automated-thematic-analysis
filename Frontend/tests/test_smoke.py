"""Smoke tests for the Frontend scaffolding. Verifies the app factory, every
registered route, and the upload form's hint rendering."""


def test_app_factory_returns_flask_app(app):
    assert app is not None
    expected_routes = {
        "/", "/health",
        "/legal-notices",
        "/transcripts/", "/transcripts/upload",
        "/transcripts/<corpus_id>/", "/transcripts/<corpus_id>/upload",
        "/codebooks/", "/codebooks/<codebook_id>/themes",
        "/analysis/",
    }
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


def test_legal_notices_render(client):
    resp = client.get("/legal-notices")
    assert resp.status_code == 200
    assert b"Legal Notices" in resp.data
    assert b"bootstrap" in resp.data
    assert b"sbom.cdx.json" in resp.data


def test_upload_form_renders_with_config(client, fake_backend):
    """Form must reflect the configured size limit and accepted extensions.

    /transcripts/upload is a landing that 302-redirects to the corpus-scoped
    form; follow_redirects=True chases it to the rendered form. The fake_backend
    fixture provides the resolved corpus_id (FakeBackend.ensure_corpus)."""
    resp = client.get("/transcripts/upload", follow_redirects=True)
    assert resp.status_code == 200
    assert b"Upload Interview Transcripts" in resp.data
    assert b"10 MB" in resp.data
    for ext in (b".txt", b".docx", b".pdf", b".jsonl"):
        assert ext in resp.data
