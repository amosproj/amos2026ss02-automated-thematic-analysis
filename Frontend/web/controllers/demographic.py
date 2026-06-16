from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from web.services.backend_client import (
    BackendError,
    BackendNotFoundError,
    BackendValidationError,
    get_backend_client as _backend,
)
from web.services.corpus_context import resolve_active_corpus, set_active_corpus_id

bp = Blueprint("demographic", __name__)



# ---- Landings ---------------------------------------------------------------


def _landing_with_corpus(target_endpoint: str):
    """Resolve the default corpus then redirect to a corpus-scoped view."""
    try:
        corpus_id, _, _ = resolve_active_corpus(_backend())
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
    # The upload entry point moved to the Uploads page 
    # as a redirect so any external links to /demographic/upload still work.
    return _landing_with_corpus("ingestion.upload_form")


# ---- List (corpus-scoped) ---------------------------------------------------


@bp.get("/<corpus_id>/")
def list_files(corpus_id: str) -> str:
    set_active_corpus_id(corpus_id)
    corpus_name = current_app.config["DEFAULT_CORPUS_NAME"]
    corpus_options: list[dict] = [{"id": corpus_id, "name": corpus_name}]
    try:
        client = _backend()
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            client,
            requested_corpus_id=corpus_id,
        )
        files = client.list_demographic_files(active_corpus_id)
        corpus_name = active_corpus.get("name", corpus_name)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "demographic/list.html",
            files=[],
            corpus_id=corpus_id,
            corpus_options=corpus_options,
            error=True,
            corpus_name=corpus_name,
        )
    return render_template(
        "demographic/list.html",
        files=files,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        corpus_name=corpus_name,
    )


# ---- Upload (corpus-scoped) ------------------------------------------------
#
# The upload entry point lives on the Uploads page : "The
# Uploads page includes a secondary file input specifically for Demographic
# Data (.csv)"). The standalone demographic upload page was removed; the
# old `/demographic/<id>/upload` URL is kept as a redirect for any external
# links. The POST endpoint stays here — the form on the Uploads page submits
# to it.


def _redirect_to_upload_form(corpus_id: str):
    """All upload-side error paths land back on the shared Uploads page so
    the demographic flash appears next to the form the user just submitted."""
    return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))


def _file_size(fileobj) -> int:
    stream = fileobj.stream
    pos = stream.tell()
    stream.seek(0, 2)
    size = stream.tell()
    stream.seek(pos)
    return size


@bp.get("/<corpus_id>/upload")
def upload_form(corpus_id: str):
    # Backwards-compatible redirect for old direct links.
    return _redirect_to_upload_form(corpus_id)


@bp.post("/<corpus_id>/upload")
def upload_submit(corpus_id: str):
    set_active_corpus_id(corpus_id)
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Please select a CSV file to upload.", "danger")
        return _redirect_to_upload_form(corpus_id)

    max_bytes = current_app.config["MAX_UPLOAD_BYTES"]
    if _file_size(f) > max_bytes:
        max_mb = current_app.config["MAX_UPLOAD_SIZE_MB"]
        flash(f"File must be at most {max_mb} MB.", "danger")
        return _redirect_to_upload_form(corpus_id)

    import_name = (request.form.get("name") or "").strip() or None

    try:
        result = _backend().upload_demographic(corpus_id, f, name=import_name)
    except BackendValidationError as exc:
        flash(exc.user_message, "danger")
        return _redirect_to_upload_form(corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return _redirect_to_upload_form(corpus_id)

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
    set_active_corpus_id(corpus_id)
    preview_data = session.get(f"demo_preview_{import_id}")
    if not preview_data:
        flash("Preview data expired or not found. Please upload again.", "warning")
        # Skip the legacy redirect through demographic.upload_form — go
        # straight to the Uploads page where the form actually lives now.
        return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))
    return render_template(
        "demographic/preview.html",
        corpus_id=corpus_id,
        import_id=import_id,
        preview=preview_data,
    )


@bp.post("/<corpus_id>/preview/<import_id>")
def preview_confirm(corpus_id: str, import_id: str):
    set_active_corpus_id(corpus_id)
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


@bp.post("/<corpus_id>/delete/<file_id>")
def delete_file(corpus_id: str, file_id: str):
    set_active_corpus_id(corpus_id)
    try:
        _backend().delete_demographic_file(corpus_id, file_id)
        flash("Demographic data deleted successfully.", "success")
    except BackendError as exc:
        flash(exc.user_message, "danger")

    return redirect(url_for("demographic.list_files", corpus_id=corpus_id))


@bp.post("/<corpus_id>/delete")
def delete_selected_files(corpus_id: str):
    set_active_corpus_id(corpus_id)
    file_ids = [item_id for item_id in request.form.getlist("item_ids") if item_id]
    if not file_ids:
        flash("Select at least one demographic file to delete.", "warning")
        return redirect(url_for("demographic.list_files", corpus_id=corpus_id))

    deleted = 0
    try:
        client = _backend()
        for file_id in file_ids:
            client.delete_demographic_file(corpus_id, file_id)
            deleted += 1
        flash(f"Deleted {deleted} demographic file{'s' if deleted != 1 else ''}.", "success")
    except BackendError as exc:
        if deleted:
            flash(f"Deleted {deleted} demographic file{'s' if deleted != 1 else ''} before an error occurred.", "warning")
        flash(exc.user_message, "danger")

    return redirect(url_for("demographic.list_files", corpus_id=corpus_id))



# ---- Linking board (corpus-scoped) -----------------------------------------
#
# The linking board is the manual override UI: a two-column view of transcripts
# and demographic rows. Researchers drag a transcript onto a demographic row to
# link them, or unlink an existing pair. Linking calls are AJAX (JSON) so the
# board updates in place without a full page reload.


@bp.get("/<corpus_id>/linking")
def linking_board(corpus_id: str) -> str:
    set_active_corpus_id(corpus_id)
    corpus_name = current_app.config["DEFAULT_CORPUS_NAME"]
    corpus_options: list[dict] = [{"id": corpus_id, "name": corpus_name}]
    error_kwargs = dict(
        transcripts=[],
        demographic_rows=[],
        summary={},
        corpus_id=corpus_id,
        corpus_options=corpus_options,
        corpus_name=corpus_name,
        error=True,
    )
    try:
        client = _backend()
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            client,
            requested_corpus_id=corpus_id,
        )
        corpus_name = active_corpus.get("name", corpus_name)
        summary = client.get_demographic_link_summary(active_corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template("demographic/linking.html", **error_kwargs)

    return render_template(
        "demographic/linking.html",
        transcripts=summary.get("details", []),
        demographic_rows=summary.get("demographic_rows", []),
        summary=summary,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        corpus_name=corpus_name,
    )


def _link_summary_json(corpus_id: str, payload: dict):
    """Shape a backend link summary into the JSON the board's JS expects."""
    return jsonify(
        {
            "ok": True,
            "total_transcripts": payload.get("total_transcripts", 0),
            "matched": payload.get("matched", 0),
            "transcripts": payload.get("details", []),
            "demographic_rows": payload.get("demographic_rows", []),
        }
    )


@bp.post("/<corpus_id>/linking/link")
def link_transcript(corpus_id: str):
    set_active_corpus_id(corpus_id)
    body = request.get_json(silent=True) or {}
    document_id = (body.get("document_id") or "").strip()
    demographic_row_id = (body.get("demographic_row_id") or "").strip()
    if not document_id or not demographic_row_id:
        return jsonify({"ok": False, "error": "document_id and demographic_row_id are required."}), 400

    try:
        summary = _backend().link_transcript(corpus_id, document_id, demographic_row_id)
    except BackendNotFoundError as exc:
        return jsonify({"ok": False, "error": exc.user_message}), 404
    except BackendValidationError as exc:
        return jsonify({"ok": False, "error": exc.user_message}), 422
    except BackendError as exc:
        return jsonify({"ok": False, "error": exc.user_message}), 502

    return _link_summary_json(corpus_id, summary)


@bp.post("/<corpus_id>/linking/unlink")
def unlink_transcript(corpus_id: str):
    set_active_corpus_id(corpus_id)
    body = request.get_json(silent=True) or {}
    document_id = (body.get("document_id") or "").strip()
    if not document_id:
        return jsonify({"ok": False, "error": "document_id is required."}), 400

    try:
        summary = _backend().unlink_transcript(corpus_id, document_id)
    except BackendNotFoundError as exc:
        return jsonify({"ok": False, "error": exc.user_message}), 404
    except BackendError as exc:
        return jsonify({"ok": False, "error": exc.user_message}), 502

    return _link_summary_json(corpus_id, summary)


# ---- View (corpus-scoped) --------------------------------------------------


@bp.get("/<corpus_id>/view/<file_id>")
def view_data(corpus_id: str, file_id: str) -> str:
    set_active_corpus_id(corpus_id)
    page = request.args.get("page", 1, type=int)
    corpus_name = current_app.config["DEFAULT_CORPUS_NAME"]
    corpus_options: list[dict] = [{"id": corpus_id, "name": corpus_name}]
    # Render the same error-state shell from either except branch so the
    # template doesn't have to know which exception class fired.
    error_kwargs = dict(
        file_info=None,
        rows=[],
        meta={},
        columns=[],
        transcript_lookup={},
        corpus_id=corpus_id,
        corpus_options=corpus_options,
        corpus_name=corpus_name,
        file_id=file_id,
        error=True,
    )
    try:
        client = _backend()
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            client,
            requested_corpus_id=corpus_id,
        )
        corpus_name = active_corpus.get("name", corpus_name)
        files = client.list_demographic_files(active_corpus_id)
        rows_page = client.list_demographic_rows(active_corpus_id, file_id, page=page, page_size=100)
        rows = rows_page.get("items", [])
        meta = rows_page.get("meta", {})
        link_summary = client.get_demographic_link_summary(active_corpus_id)
    except BackendNotFoundError:
        # Specific user-facing message when a stale link points at a file
        # that no longer exists — same pattern as codebook_themes uses.
        flash(
            "That demographic file couldn't be found. It may have been deleted.",
            "danger",
        )
        return render_template("demographic/view.html", **error_kwargs)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template("demographic/view.html", **error_kwargs)

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
        meta=meta,
        columns=columns,
        transcript_lookup=transcript_lookup,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        corpus_name=corpus_name,
        file_id=file_id,
    )
