# test_error_handlers.py — covers the three Flask error handlers registered in
# web/__init__.py: 404 (unknown route), 413 (request body too large), and the
# generic Exception handler for anything views raise and forget to catch.

import io


# 404


def test_404_renders_branded_page(client):
    """A typo URL should render the custom error template, not Flask's default."""
    resp = client.get("/this-route-does-not-exist")
    assert resp.status_code == 404
    assert b"Error 404" in resp.data
    assert b"Page Not Found" in resp.data
    # The branded template extends base.html so the navbar must still be present.
    assert b"navbar" in resp.data


# 413


def test_413_when_request_body_exceeds_max_content_length(client, fake_backend, app):
    """Posting a body larger than MAX_CONTENT_LENGTH triggers the 413 handler.

    The handler issues a 303 redirect — `follow_redirects=False` keeps the test
    focused on the 303 itself; the upload form would be 200.
    """
    max_bytes = app.config["MAX_CONTENT_LENGTH"]
    body = b"x" * (max_bytes + 1024)
    resp = client.post(
        "/transcripts/test-corpus-id/upload",
        data={"files": [(io.BytesIO(body), "huge.txt")]},
        content_type="multipart/form-data",
        follow_redirects=False,
    )
    # Werkzeug raises RequestEntityTooLarge before our handler — Flask's handler
    # converts that to a 303 redirect (with a flash queued).
    assert resp.status_code == 303


def test_413_always_redirects_to_home_ignoring_referrer(client, fake_backend, app):
    """Open Redirect guard (CWE-601): the 413 handler must redirect to the
    application home page regardless of any Referer header the request carried.
    We never pass user-controlled values to redirect() so the dataflow CodeQL
    looks for (request.referrer / Host → redirect) doesn't exist."""
    max_bytes = app.config["MAX_CONTENT_LENGTH"]
    body = b"x" * (max_bytes + 1024)
    resp = client.post(
        "/transcripts/test-corpus-id/upload",
        data={"files": [(io.BytesIO(body), "huge.txt")]},
        content_type="multipart/form-data",
        headers={"Referer": "https://attacker.example.com/phish"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    location = resp.headers["Location"]
    # Strict check: the target must equal the home page URL — `endswith("/")`
    # would also accept e.g. "/transcripts/", silently hiding a regression
    # where the handler started redirecting somewhere unexpected.
    assert location in {"/", "http://localhost/"}


# Generic Exception handler


def test_unhandled_view_exception_renders_500_page(app, client):
    """Register a view that raises an unhandled exception, hit it, expect a
    branded 500 page (not a traceback)."""

    @app.route("/__force_error__")
    def _boom():
        raise RuntimeError("simulated unhandled exception")

    # Disable Flask's default "propagate exceptions in TESTING mode" behaviour
    # so our errorhandler runs instead of pytest seeing the raw RuntimeError.
    app.config["TESTING"] = False
    app.config["PROPAGATE_EXCEPTIONS"] = False

    resp = client.get("/__force_error__")
    assert resp.status_code == 500
    assert b"Error 500" in resp.data
    assert b"Something went wrong" in resp.data
    # Never leak internals.
    assert b"Traceback" not in resp.data
    assert b"simulated unhandled exception" not in resp.data
