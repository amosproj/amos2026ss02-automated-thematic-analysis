from flask import Flask

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

    return app
