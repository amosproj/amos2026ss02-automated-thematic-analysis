from flask import Blueprint, render_template

bp = Blueprint("main", __name__)


@bp.get("/")
def index() -> str:
    return render_template("index.html")


@bp.get("/health")
def health() -> dict:
    """Liveness probe for Docker HEALTHCHECK; does not check the backend."""
    return {"status": "ok"}
