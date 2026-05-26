from flask import Blueprint, render_template

bp = Blueprint("analysis", __name__)


@bp.get("/")
def index() -> str:
    return render_template("analysis/index.html")
