from flask import Blueprint, render_template

bp = Blueprint("main", __name__)


@bp.get("/")
def index() -> str:
    return render_template("index.html")


@bp.get("/health")
def health() -> dict:
    """Liveness probe. Used by Docker HEALTHCHECK and ops tooling. Does not call
    the backend — only checks that the frontend process itself is responsive."""
    return {"status": "ok"}
