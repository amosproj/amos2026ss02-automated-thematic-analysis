from flask import (
    Blueprint,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from web.services.backend_client import (
    BackendClient,
    BackendError,
    BackendValidationError,
    get_backend_client as _backend,
)

bp = Blueprint("demographic", __name__)


def _resolve_workspace_corpus(client: BackendClient) -> str:
    """MVP single-workspace: resolve or create the default corpus."""
    cfg = current_app.config
    return client.ensure_corpus(
        project_id=cfg["DEFAULT_PROJECT_ID"],
        name=cfg["DEFAULT_CORPUS_NAME"],
    )


# ---- Landings ---------------------------------------------------------------


def _landing_with_corpus(target_endpoint: str):
    """Resolve the default corpus then redirect to a corpus-scoped view."""
    try:
        corpus_id = _resolve_workspace_corpus(_backend())
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "demographic/list.html", files=[], corpus_id=None, error=True
        )
    return redirect(url_for(target_endpoint, corpus_id=corpus_id))


@bp.get("/")
def demographic_landing():
    return _landing_with_corpus("demographic.list_files")


@bp.get("/upload")
def upload_landing():
    return _landing_with_corpus("demographic.upload_form")


# ---- List (corpus-scoped) ---------------------------------------------------


@bp.get("/<corpus_id>/")
def list_files(corpus_id: str) -> str:
    try:
        files = _backend().list_demographic_files(corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "demographic/list.html", files=[], corpus_id=corpus_id, error=True
        )
    return render_template(
        "demographic/list.html", files=files, corpus_id=corpus_id
    )


# ---- Upload (corpus-scoped) ------------------------------------------------


def _render_upload_form(corpus_id: str) -> str:
    return render_template(
        "demographic/upload.html",
        corpus_id=corpus_id,
        max_size_mb=current_app.config["MAX_UPLOAD_SIZE_MB"],
    )


def _file_size(fileobj) -> int:
    stream = fileobj.stream
    pos = stream.tell()
    stream.seek(0, 2)
    size = stream.tell()
    stream.seek(pos)
    return size


@bp.get("/<corpus_id>/upload")
def upload_form(corpus_id: str) -> str:
    return _render_upload_form(corpus_id)


@bp.post("/<corpus_id>/upload")
def upload_submit(corpus_id: str):
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Please select a CSV file to upload.", "danger")
        return _render_upload_form(corpus_id)

    max_bytes = current_app.config["MAX_UPLOAD_BYTES"]
    if _file_size(f) > max_bytes:
        max_mb = current_app.config["MAX_UPLOAD_SIZE_MB"]
        flash(f"File must be at most {max_mb} MB.", "danger")
        return _render_upload_form(corpus_id)

    try:
        result = _backend().upload_demographic(corpus_id, f)
    except BackendValidationError as exc:
        flash(exc.user_message, "danger")
        return _render_upload_form(corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return _render_upload_form(corpus_id)

    # Store preview data in the session so the preview page can display it.
    import_id = result["import_id"]
    session[f"demo_preview_{import_id}"] = result
    return redirect(
        url_for(
            "demographic.preview_upload",
            corpus_id=corpus_id,
            import_id=import_id,
        )
    )


# ---- Preview (corpus-scoped) -----------------------------------------------


@bp.get("/<corpus_id>/preview/<import_id>")
def preview_upload(corpus_id: str, import_id: str) -> str:
    preview_data = session.get(f"demo_preview_{import_id}")
    if not preview_data:
        flash("Preview data expired or not found. Please upload again.", "warning")
        return redirect(url_for("demographic.upload_form", corpus_id=corpus_id))
    return render_template(
        "demographic/preview.html",
        corpus_id=corpus_id,
        import_id=import_id,
        preview=preview_data,
    )


@bp.post("/<corpus_id>/preview/<import_id>")
def preview_confirm(corpus_id: str, import_id: str):
    action = request.form.get("action", "discard")
    confirm = action == "confirm"

    # Clear session data regardless of outcome.
    session.pop(f"demo_preview_{import_id}", None)

    try:
        _backend().confirm_demographic(corpus_id, import_id, confirm)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("demographic.list_files", corpus_id=corpus_id))

    if confirm:
        flash("Demographic data uploaded successfully.", "success")
    else:
        flash("Upload discarded.", "info")
    return redirect(url_for("demographic.list_files", corpus_id=corpus_id))


# ---- View (corpus-scoped) --------------------------------------------------


@bp.get("/<corpus_id>/view/<file_id>")
def view_data(corpus_id: str, file_id: str) -> str:
    try:
        client = _backend()
        files = client.list_demographic_files(corpus_id)
        rows = client.list_demographic_rows(corpus_id, file_id)
        link_summary = client.get_demographic_link_summary(corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "demographic/view.html",
            file_info=None,
            rows=[],
            columns=[],
            transcript_lookup={},
            corpus_id=corpus_id,
            file_id=file_id,
            error=True,
        )

    # Find the specific file metadata.
    file_info = next((f for f in files if f["id"] == file_id), None)

    # Build a lookup: demographic_row_id → document_title
    transcript_lookup = {}
    for detail in link_summary.get("details", []):
        if detail.get("matched") and detail.get("demographic_row_id"):
            transcript_lookup[detail["demographic_row_id"]] = detail["document_title"]

    # Extract column names from file metadata (excluding 'username').
    columns = []
    if file_info:
        columns = [c for c in file_info.get("original_columns", []) if c != "username"]

    return render_template(
        "demographic/view.html",
        file_info=file_info,
        rows=rows,
        columns=columns,
        transcript_lookup=transcript_lookup,
        corpus_id=corpus_id,
        file_id=file_id,
    )
