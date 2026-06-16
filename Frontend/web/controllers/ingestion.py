import uuid
from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from web.services.backend_client import (
    BackendError,
    BackendValidationError,
    get_backend_client as _backend,
)
from web.services.corpus_context import (
    resolve_active_corpus,
    set_active_corpus_id,
)

bp = Blueprint("ingestion", __name__)



# Landings — resolve the default corpus then redirect to the corpus-scoped URL.


def _landing_with_corpus(target_endpoint: str):
    """Both landings share the same shape: resolve the default corpus, then
    redirect to a corpus-scoped view. On backend failure, render the empty
    transcript list with the user-facing error message — the only sensible
    fallback view when we don't yet know a corpus id."""
    try:
        corpus_id, _, _ = resolve_active_corpus(_backend())
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "ingestion/list.html", documents=[], corpus_id=None, error=True
        )
    return redirect(url_for(target_endpoint, corpus_id=corpus_id))


@bp.get("/")
def transcripts_landing():
    return _landing_with_corpus("ingestion.list_transcripts")


@bp.get("/upload")
def upload_landing():
    return _landing_with_corpus("ingestion.upload_form")

# Upload (corpus-scoped)



def _render_upload_form(corpus_id: str) -> str:
    cfg = current_app.config
    try:
        client = _backend()
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            client,
            requested_corpus_id=corpus_id,
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        active_corpus_id = corpus_id
        active_corpus = {"id": corpus_id, "name": cfg["DEFAULT_CORPUS_NAME"]}
        corpus_options = [active_corpus]

    return render_template(
        "ingestion/upload.html",
        corpus_id=active_corpus_id,
        active_corpus_name=active_corpus.get("name"),
        corpus_options=corpus_options,
        max_size_mb=cfg["MAX_UPLOAD_SIZE_MB"],
        accepted_extensions=sorted(cfg["ACCEPTED_EXTENSIONS"]),
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
    set_active_corpus_id(corpus_id)
    return _render_upload_form(corpus_id)


@bp.post("/corpora")
def create_corpus_submit():
    """Create a new corpus from the uploads page selector flow.

    The backend owns UUID creation; the frontend only collects a corpus name.
    On success, redirect to the upload page scoped to the new corpus so both
    transcript and demographic forms target it immediately.
    """
    name = (request.form.get("name") or "").strip()
    current_corpus_id = (request.form.get("current_corpus_id") or "").strip()

    if not name:
        flash("Please enter a corpus name.", "danger")
        if current_corpus_id:
            return redirect(url_for("ingestion.upload_form", corpus_id=current_corpus_id))
        return redirect(url_for("ingestion.upload_landing"))

    try:
        new_id = str(uuid.uuid4())
        created = _backend().create_corpus(
            corpus_id=new_id,
            name=name,
        )
    except BackendValidationError as exc:
        flash(exc.user_message, "danger")
        if current_corpus_id:
            return redirect(url_for("ingestion.upload_form", corpus_id=current_corpus_id))
        return redirect(url_for("ingestion.upload_landing"))
    except BackendError as exc:
        flash(exc.user_message, "danger")
        if current_corpus_id:
            return redirect(url_for("ingestion.upload_form", corpus_id=current_corpus_id))
        return redirect(url_for("ingestion.upload_landing"))

    new_corpus_id = created["id"]
    set_active_corpus_id(new_corpus_id)
    flash(f"Created corpus '{created.get('name', name)}'.", "success")
    return redirect(url_for("ingestion.upload_form", corpus_id=new_corpus_id))


@bp.post("/<corpus_id>/delete")
def delete_corpus_submit(corpus_id: str):
    """Delete a corpus and redirect to landing page."""
    try:
        _backend().delete_corpus(corpus_id)
        flash("Corpus deleted successfully.", "success")
        # Clear the active corpus ID from the session as it no longer exists
        set_active_corpus_id(None)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))
    
    return redirect(url_for("ingestion.transcripts_landing"))


@bp.post("/<corpus_id>/upload")
def upload_submit(corpus_id: str) -> str:
    set_active_corpus_id(corpus_id)
    active_corpus_name = current_app.config["DEFAULT_CORPUS_NAME"]
    corpus_options: list[dict] = [{"id": corpus_id, "name": active_corpus_name}]
    active_corpus_id = corpus_id
    try:
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            _backend(),
            requested_corpus_id=corpus_id,
        )
        active_corpus_name = active_corpus.get("name", active_corpus_name)
    except BackendError as exc:
        flash(exc.user_message, "danger")

    files = [f for f in request.files.getlist("files") if f and f.filename]
    if not files:
        flash("Please select at least one file to upload.", "danger")
        return _render_upload_form(active_corpus_id)

    max_bytes = current_app.config["MAX_UPLOAD_BYTES"]
    oversize = [f.filename for f in files if _file_size(f) > max_bytes]
    if oversize:
        max_mb = current_app.config["MAX_UPLOAD_SIZE_MB"]
        flash(
            f"Each file must be at most {max_mb} MB. Too large: {', '.join(oversize)}.",
            "danger",
        )
        return _render_upload_form(active_corpus_id)

    try:
        results = _backend().upload_files(active_corpus_id, files)
    except BackendValidationError as exc:
        # Backend rejected the payload — re-render the form so the user
        # can fix it. The validation message names the offending field.
        flash(exc.user_message, "danger")
        return _render_upload_form(active_corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "ingestion/results.html",
            results=[],
            corpus_id=active_corpus_id,
            corpus_options=corpus_options,
            active_corpus_name=active_corpus_name,
            error=True,
        )

    return render_template(
        "ingestion/results.html",
        results=results,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        active_corpus_name=active_corpus_name,
    )

# List (corpus-scoped)


def _build_link_status(client, corpus_id: str) -> dict:
    """Map document_id -> {linked, interviewee_id} from the demographic link summary.

    Best-effort: any backend failure yields an empty map so the transcript list
    still renders. Linking is an optional, separately-uploaded concern.
    """
    try:
        summary = client.get_demographic_link_summary(corpus_id)
    except BackendError:
        return {}

    rows_by_id = {
        r["row_id"]: r.get("interviewee_id")
        for r in summary.get("demographic_rows", [])
    }
    status: dict[str, dict] = {}
    for detail in summary.get("details", []):
        row_id = detail.get("demographic_row_id")
        status[detail["document_id"]] = {
            "linked": bool(detail.get("matched")),
            "interviewee_id": rows_by_id.get(row_id) if row_id else None,
        }
    return status


@bp.get("/<corpus_id>/")
def list_transcripts(corpus_id: str) -> str:
    set_active_corpus_id(corpus_id)
    active_corpus_name = current_app.config["DEFAULT_CORPUS_NAME"]
    corpus_options: list[dict] = [{"id": corpus_id, "name": active_corpus_name}]
    try:
        client = _backend()
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            client,
            requested_corpus_id=corpus_id,
        )
        documents = client.list_documents(active_corpus_id)
        active_corpus_name = active_corpus.get("name", active_corpus_name)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "ingestion/list.html",
            documents=[],
            corpus_id=corpus_id,
            corpus_options=corpus_options,
            active_corpus_name=active_corpus_name,
            error=True,
        )
    link_status = _build_link_status(client, active_corpus_id)
    return render_template(
        "ingestion/list.html",
        documents=documents,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        active_corpus_name=active_corpus_name,
        link_status=link_status,
    )


@bp.post("/<corpus_id>/<document_id>/delete")
def delete_transcript(corpus_id: str, document_id: str):
    """Delete a single transcript from the active corpus."""
    set_active_corpus_id(corpus_id)
    try:
        _backend().delete_document(corpus_id, document_id)
        flash("Transcript deleted successfully.", "success")
    except BackendError as exc:
        flash(exc.user_message, "danger")
    return redirect(url_for("ingestion.list_transcripts", corpus_id=corpus_id))


@bp.post("/<corpus_id>/delete_transcripts")
def delete_selected_transcripts(corpus_id: str):
    """Delete transcripts selected in the list view."""
    set_active_corpus_id(corpus_id)
    document_ids = [item_id for item_id in request.form.getlist("item_ids") if item_id]
    if not document_ids:
        flash("Select at least one transcript to delete.", "warning")
        return redirect(url_for("ingestion.list_transcripts", corpus_id=corpus_id))

    deleted = 0
    try:
        client = _backend()
        for document_id in document_ids:
            client.delete_document(corpus_id, document_id)
            deleted += 1
        flash(f"Deleted {deleted} transcript{'s' if deleted != 1 else ''}.", "success")
    except BackendError as exc:
        if deleted:
            flash(f"Deleted {deleted} transcript{'s' if deleted != 1 else ''} before an error occurred.", "warning")
        flash(exc.user_message, "danger")

    return redirect(url_for("ingestion.list_transcripts", corpus_id=corpus_id))


@bp.get("/<corpus_id>/<document_id>/read")
def read_transcript(corpus_id: str, document_id: str) -> str:
    set_active_corpus_id(corpus_id)
    try:
        document = _backend().get_document_content(corpus_id, document_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("ingestion.list_transcripts", corpus_id=corpus_id))

    return render_template(
        "ingestion/read.html",
        document=document,
        corpus_id=corpus_id,
    )
