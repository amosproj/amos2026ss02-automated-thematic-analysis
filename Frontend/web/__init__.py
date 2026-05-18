import atexit
import logging

from flask import Flask, current_app, flash, redirect, render_template, url_for

from web.config import Config, get_config
from web.services.backend_client import BackendClient


def create_app(config: Config | None = None) -> Flask:
    app = Flask(__name__)
    cfg = config or get_config()
    app.config.from_object(cfg)

    # Cap raw request body size so oversized uploads short-circuit with a 413
    # before Werkzeug buffers the whole body into memory.
    app.config["MAX_CONTENT_LENGTH"] = cfg.MAX_CONTENT_LENGTH

    # Propagate the configured LOG_LEVEL to Flask's logger so backend_client
    # log lines actually appear (Flask defaults to WARNING in production).
    log_level = getattr(logging, cfg.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(level=log_level)
    app.logger.setLevel(log_level)

    # Shared across requests: pooled connections + memoised corpus id.
    # `atexit` (not `teardown_appcontext`) is the right hook here — the latter
    # fires on every request and would close the pooled client we want to reuse.
    backend_client = BackendClient(
        cfg.BACKEND_API_URL, timeout=cfg.BACKEND_TIMEOUT_S
    )
    app.extensions["backend_client"] = backend_client
    atexit.register(backend_client.close)

    from web.controllers.analysis import bp as analysis_bp
    from web.controllers.codebooks import bp as codebooks_bp
    from web.controllers.codebook import bp as codebook_bp
    from web.controllers.ingestion import bp as ingestion_bp
    from web.controllers.main import bp as main_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/transcripts")
    app.register_blueprint(codebooks_bp, url_prefix="/codebooks")
    app.register_blueprint(codebook_bp)
    app.register_blueprint(analysis_bp, url_prefix="/analysis")

    _register_error_handlers(app)

    return app


def _register_error_handlers(app: Flask) -> None:
    """Three handlers cover every uncaught case:
    404 (unknown route), 413 (request body too large), and a catch-all
    Exception handler for anything view code raises and forgets to catch.
    """

    @app.errorhandler(404)
    def not_found(_exc):
        return render_template("errors/404.html"), 404

    @app.errorhandler(413)
    def request_too_large(_exc):
        max_mb = current_app.config["MAX_UPLOAD_SIZE_MB"] * 10
        flash(
            f"Upload too large — the total request exceeded {max_mb} MB.",
            "danger",
        )
        # 303 forces a GET on the redirect, so the browser doesn't try to re-POST.
        # Always redirect to home — never to a user-controlled value (e.g.
        # request.referrer) — to eliminate the open-redirect risk (CWE-601).
        # CodeQL's py/url-redirection rule recognises only a small set of
        # sanitiser patterns (strict allowlist, empty-netloc-and-scheme
        # relative URLs, Django's url_has_allowed_host_and_scheme). A
        # hardcoded url_for() target sidesteps the dataflow entirely.
        return redirect(url_for("main.index")), 303

    @app.errorhandler(Exception)
    def unhandled(exc):
        # Re-raise HTTPException so Flask's built-in handlers (and our 404/413
        # above) still run — only catch genuinely unexpected exceptions here.
        from werkzeug.exceptions import HTTPException

        if isinstance(exc, HTTPException):
            return exc
        current_app.logger.exception("Unhandled view exception")
        return render_template("errors/500.html"), 500
