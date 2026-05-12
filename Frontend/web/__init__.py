from flask import Flask, render_template

from web.config import Config, get_config


def create_app(config: Config | None = None) -> Flask:
    app = Flask(
        __name__,
        template_folder="../templates",
        static_folder="../static",
    )
    app.config.from_object(config or get_config())

    from web.controllers.ingestion import bp as ingestion_bp
    from web.controllers.main import bp as main_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(ingestion_bp, url_prefix="/transcripts")

    # Friendly page for oversize uploads (Flask rejects them pre-handler).
    @app.errorhandler(413)
    def _too_large(_err):
        max_mb = app.config["MAX_UPLOAD_SIZE_MB"]
        return render_template(
            "ingestion/results.html",
            results=[],
            error=f"Total upload exceeded {max_mb} MB. Please upload fewer or smaller files.",
        ), 413

    return app
