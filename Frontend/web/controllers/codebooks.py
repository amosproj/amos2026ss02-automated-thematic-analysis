from flask import Blueprint, flash, render_template, request, redirect, url_for, current_app

from web.services.backend_client import (
    BackendError,
    BackendNotFoundError,
    get_backend_client as _backend,
)

bp = Blueprint("codebooks", __name__)


@bp.get("/")
def list_codebooks() -> str:
    try:
        codebooks = _backend().list_codebooks()
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template("codebooks/list.html", codebooks=[], error=True)
    return render_template("codebooks/list.html", codebooks=codebooks)


@bp.get("/<codebook_id>/themes")
def codebook_themes(codebook_id: str) -> str:
    name = request.args.get("name", "")
    version = request.args.get("version", "")
    try:
        client = _backend()
        frequencies = client.get_theme_frequencies(codebook_id)
        tree = client.get_theme_tree(codebook_id)
    except BackendNotFoundError:
        flash(
            "That codebook couldn't be found. It may have been deleted.",
            "danger",
        )
        return render_template(
            "codebooks/themes.html",
            codebook_id=codebook_id,
            name=name,
            version=version,
            frequencies=[],
            tree=[],
            error=True,
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "codebooks/themes.html",
            codebook_id=codebook_id,
            name=name,
            version=version,
            frequencies=[],
            tree=[],
            error=True,
        )
    return render_template(
        "codebooks/themes.html",
        codebook_id=codebook_id,
        name=name,
        version=version,
        frequencies=frequencies,
        tree=tree,
    )

@bp.get("/upload")
def upload_form() -> str:
    """Render the upload form (choose CSV or manual)."""
    return render_template("codebooks/upload.html", error=None)

@bp.post("/upload")
def upload_submit() -> str:
    """Handle either CSV file upload or redirect to manual entry."""
    action = request.form.get("action", "upload")

    if action == "manual":
        return redirect(url_for("codebooks.manual_form"))

    # CSV file upload path
    file = request.files.get("file")
    if not file or not file.filename:
        return render_template(
            "codebooks/upload.html",
            error="Please select a CSV file to upload or choose manual entry.",
        )

    if not file.filename.lower().endswith(".csv"):
        return render_template(
            "codebooks/upload.html",
            error="Only CSV files (.csv extension) are supported.",
        )

    try:
        client = _backend()
        parsed_themes = client.parse_csv_preview(file)
        # Derive a readable default name from the file name
        default_name = file.filename.rsplit(".", 1)[0].replace("_", " ").title()
        return render_template(
            "codebooks/preview.html",
            codebook_name=default_name,
            themes=parsed_themes,
            error=None,
        )
    except BackendError as exc:
        return render_template("codebooks/upload.html", error=str(exc))

@bp.get("/manual")
def manual_form() -> str:
    """Render the preview editor pre-filled with one blank theme row."""
    empty_themes = [{"node_type": "THEME", "name": "", "description": "", "parent_name": ""}]
    return render_template(
        "codebooks/preview.html",
        codebook_name="New Codebook",
        themes=empty_themes,
        error=None,
    )

@bp.post("/confirm")
def confirm_submit() -> str:
    """Validate, customise, and confirm a codebook and its themes."""
    codebook_name = (request.form.get("codebook_name") or "").strip()
    node_types = request.form.getlist("node_types[]")
    theme_names = request.form.getlist("theme_names[]")
    theme_descriptions = request.form.getlist("theme_descriptions[]")
    parent_names = request.form.getlist("parent_names[]")

    # Assemble themes back into expected structure
    themes = []
    for nt, name, desc, parent in zip(node_types, theme_names, theme_descriptions, parent_names):
        themes.append({
            "node_type": nt,
            "name": name.strip(),
            "description": desc.strip(),
            "parent_name": parent.strip() if parent.strip() else None
        })

    # Frontend validation
    error = None
    if not codebook_name:
        error = "Codebook Name must not be blank."
    elif not themes:
        error = "A codebook must contain at least one theme."
    elif any(not t["name"] for t in themes):
        error = "All themes must have a name."

    if error:
        return render_template(
            "codebooks/preview.html",
            codebook_name=codebook_name,
            themes=themes,
            error=error,
        )

    try:
        client = _backend()
        project_id = current_app.config["DEFAULT_PROJECT_ID"]
        res = client.create_codebook(project_id, codebook_name, themes)
        codebook_id = res["id"]
        return redirect(url_for("codebooks.success", codebook_id=codebook_id))
    except BackendError as exc:
        return render_template(
            "codebooks/preview.html",
            codebook_name=codebook_name,
            themes=themes,
            error=str(exc),
        )

@bp.get("/success")
def success() -> str:
    """Show details of the successfully saved codebook."""
    codebook_id = request.args.get("codebook_id")
    if not codebook_id:
        return redirect(url_for("codebooks.upload_form"))

    try:
        client = _backend()
        codebook = client.get_codebook(codebook_id)
        return render_template("codebooks/success.html", codebook=codebook, error=None)
    except BackendError as exc:
        return render_template("codebooks/success.html", codebook=None, error=str(exc))
