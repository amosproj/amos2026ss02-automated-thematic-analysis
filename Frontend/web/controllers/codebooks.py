from flask import Blueprint, current_app, flash, render_template, request

from web.services.backend_client import BackendClient, BackendError

bp = Blueprint("codebooks", __name__)


def _backend() -> BackendClient:
    return current_app.extensions["backend_client"]


@bp.get("/")
def list_codebooks() -> str:
    try:
        codebooks = _backend().list_codebooks()
    except BackendError as exc:
        flash(str(exc), "danger")
        return render_template("codebooks/list.html", codebooks=[])
    return render_template("codebooks/list.html", codebooks=codebooks)


@bp.get("/<codebook_id>/themes")
def codebook_themes(codebook_id: str) -> str:
    name = request.args.get("name", "")
    version = request.args.get("version", "")
    try:
        client = _backend()
        frequencies = client.get_theme_frequencies(codebook_id)
        tree = client.get_theme_tree(codebook_id)
    except BackendError as exc:
        flash(str(exc), "danger")
        return render_template(
            "codebooks/themes.html",
            codebook_id=codebook_id,
            name=name,
            version=version,
            frequencies=[],
            tree=[],
        )
    return render_template(
        "codebooks/themes.html",
        codebook_id=codebook_id,
        name=name,
        version=version,
        frequencies=frequencies,
        tree=tree,
    )
