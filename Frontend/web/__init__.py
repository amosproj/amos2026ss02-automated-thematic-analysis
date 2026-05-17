from flask import Flask

from web.config import Config, get_config
from web.services.backend_client import BackendClient


def create_app(config: Config | None = None) -> Flask:
    app = Flask(__name__)
    cfg = config or get_config()
    app.config.from_object(cfg)

    # Shared across requests: pooled connections + memoised corpus id.
    app.extensions["backend_client"] = BackendClient(
        cfg.BACKEND_API_URL, timeout=cfg.BACKEND_TIMEOUT_S
    )

    from web.controllers.analysis import bp as analysis_bp
    from web.controllers.codebooks import bp as codebooks_bp
    from web.controllers.ingestion import bp as ingestion_bp
    from web.controllers.main import bp as main_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/transcripts")
    app.register_blueprint(codebooks_bp, url_prefix="/codebooks")
    app.register_blueprint(analysis_bp, url_prefix="/analysis")

    return app
