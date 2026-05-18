from flask import Blueprint, current_app, redirect, render_template, request, url_for

from web.services.backend_client import BackendClient, BackendError

bp = Blueprint("codebook", __name__, url_prefix="/codebooks")


def _backend() -> BackendClient:
    return current_app.extensions["backend_client"]


# ---------------------------------------------------------------------------
# List Codebooks
# ---------------------------------------------------------------------------


@bp.get("/")
def list_codebooks() -> str:
    """Show all saved codebooks with their theme counts."""
    try:
        client = _backend()
        codebooks = client.list_codebooks()
    except BackendError as exc:
        return render_template("codebook/list.html", codebooks=[], error=str(exc))
    return render_template("codebook/list.html", codebooks=codebooks, error=None)


# ---------------------------------------------------------------------------
# Upload / Manual Entry Selection Screen
# ---------------------------------------------------------------------------


@bp.get("/upload")
def upload_form() -> str:
    """Render the upload form (choose CSV or manual)."""
    return render_template("codebook/upload.html", error=None)


@bp.post("/upload")
def upload_submit() -> str:
    """Handle either CSV file upload or redirect to manual entry."""
    action = request.form.get("action", "upload")

    if action == "manual":
        return redirect(url_for("codebook.manual_form"))

    # CSV file upload path
    file = request.files.get("file")
    if not file or not file.filename:
        return render_template(
            "codebook/upload.html",
            error="Please select a CSV file to upload or choose manual entry.",
        )

    if not file.filename.lower().endswith(".csv"):
        return render_template(
            "codebook/upload.html",
            error="Only CSV files (.csv extension) are supported.",
        )

    try:
        client = _backend()
        parsed_themes = client.parse_csv_preview(file)
        # Derive a readable default name from the file name
        default_name = file.filename.rsplit(".", 1)[0].replace("_", " ").title()
        return render_template(
            "codebook/preview.html",
            codebook_name=default_name,
            themes=parsed_themes,
            error=None,
        )
    except BackendError as exc:
        return render_template("codebook/upload.html", error=str(exc))


# ---------------------------------------------------------------------------
# Manual Entry
# ---------------------------------------------------------------------------


@bp.get("/manual")
def manual_form() -> str:
    """Render the preview editor pre-filled with one blank theme row."""
    empty_themes = [{"name": "", "description": ""}]
    return render_template(
        "codebook/preview.html",
        codebook_name="New Codebook",
        themes=empty_themes,
        error=None,
    )


# ---------------------------------------------------------------------------
# Preview & Customise & Confirm Screen
# ---------------------------------------------------------------------------


@bp.post("/confirm")
def confirm_submit() -> str:
    """Validate, customise, and confirm a codebook and its themes."""
    codebook_name = (request.form.get("codebook_name") or "").strip()
    theme_names = request.form.getlist("theme_names[]")
    theme_descriptions = request.form.getlist("theme_descriptions[]")

    # Assemble themes back into expected structure
    themes = []
    for name, desc in zip(theme_names, theme_descriptions):
        themes.append({"name": name.strip(), "description": desc.strip()})

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
            "codebook/preview.html",
            codebook_name=codebook_name,
            themes=themes,
            error=error,
        )

    try:
        client = _backend()
        project_id = current_app.config["DEFAULT_PROJECT_ID"]
        res = client.create_codebook(project_id, codebook_name, themes)
        codebook_id = res["id"]
        return redirect(url_for("codebook.success", codebook_id=codebook_id))
    except BackendError as exc:
        return render_template(
            "codebook/preview.html",
            codebook_name=codebook_name,
            themes=themes,
            error=str(exc),
        )


@bp.get("/success")
def success() -> str:
    """Show details of the successfully saved codebook."""
    codebook_id = request.args.get("codebook_id")
    if not codebook_id:
        return redirect(url_for("codebook.upload_form"))

    try:
        client = _backend()
        codebook = client.get_codebook(codebook_id)
        return render_template("codebook/success.html", codebook=codebook, error=None)
    except BackendError as exc:
        return render_template("codebook/success.html", codebook=None, error=str(exc))
