import uuid
from flask import Blueprint, current_app, flash, redirect, render_template, request, session, url_for

from web.services.backend_client import (
    BackendConflictError,
    BackendError,
    BackendValidationError,
    get_backend_client as _backend,
)
from web.services.corpus_context import (
    resolve_active_corpus,
    set_active_corpus_id,
)

bp = Blueprint("ingestion", __name__)

_PENDING_TRANSCRIPT_DELETE_KEY = "pending_transcript_delete"
_PENDING_CORPUS_DELETE_KEY = "pending_corpus_delete"



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



def _pending_corpus_delete(corpus_id: str) -> dict | None:
    pending = session.pop(_PENDING_CORPUS_DELETE_KEY, None)
    if not isinstance(pending, dict) or pending.get("corpus_id") != corpus_id:
        return None
    return {
        "message": pending.get("message") or "Deleting this corpus would interrupt a running analysis.",
        "item_ids": [],
        "action": url_for("ingestion.delete_corpus_submit", corpus_id=corpus_id),
        "title": "Delete Corpus",
        "confirm_label": "Yes, Delete Corpus",
    }


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
        pending_analysis_delete=_pending_corpus_delete(active_corpus_id),
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
    force = request.form.get("force_delete") == "1"
    try:
        _backend().delete_corpus(corpus_id, force=force)
        flash("Corpus deleted successfully.", "success")
        # Clear the active corpus ID from the session as it no longer exists
        set_active_corpus_id(None)
    except BackendConflictError as exc:
        session[_PENDING_CORPUS_DELETE_KEY] = {
            "corpus_id": corpus_id,
            "message": exc.user_message,
        }
        return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))
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


def _pending_transcript_delete(corpus_id: str) -> dict | None:
    pending = session.pop(_PENDING_TRANSCRIPT_DELETE_KEY, None)
    if not isinstance(pending, dict) or pending.get("corpus_id") != corpus_id:
        return None
    item_ids = [item_id for item_id in pending.get("item_ids", []) if item_id]
    if not item_ids:
        return None
    return {
        "message": pending.get("message") or "Deleting these transcripts would interrupt a running analysis.",
        "item_ids": item_ids,
        "action": url_for("ingestion.delete_selected_transcripts", corpus_id=corpus_id),
        "title": "Delete Transcripts",
        "confirm_label": "Yes, Delete Transcripts",
    }


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
        documents = client.list_documents(active_corpus_id, page_size=10000)
        active_corpus_name = active_corpus.get("name", active_corpus_name)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "ingestion/list.html",
            documents=[],
            corpus_id=corpus_id,
            corpus_options=corpus_options,
            active_corpus_name=active_corpus_name,
            pending_analysis_delete=None,
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
        pending_analysis_delete=_pending_transcript_delete(active_corpus_id),
    )


@bp.post("/<corpus_id>/<document_id>/delete")
def delete_transcript(corpus_id: str, document_id: str):
    """Delete a single transcript from the active corpus."""
    set_active_corpus_id(corpus_id)
    force = request.form.get("force_delete") == "1"
    try:
        _backend().delete_document(corpus_id, document_id, force=force)
        flash("Transcript deleted successfully.", "success")
    except BackendConflictError as exc:
        session[_PENDING_TRANSCRIPT_DELETE_KEY] = {
            "corpus_id": corpus_id,
            "item_ids": [document_id],
            "message": exc.user_message,
        }
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

    force = request.form.get("force_delete") == "1"
    deleted = 0
    try:
        client = _backend()
        for document_id in document_ids:
            client.delete_document(corpus_id, document_id, force=force)
            deleted += 1
        flash(f"Deleted {deleted} transcript{'s' if deleted != 1 else ''}.", "success")
    except BackendConflictError as exc:
        if deleted:
            flash(f"Deleted {deleted} transcript{'s' if deleted != 1 else ''} before an error occurred.", "warning")
        if not force:
            session[_PENDING_TRANSCRIPT_DELETE_KEY] = {
                "corpus_id": corpus_id,
                "item_ids": document_ids[deleted:],
                "message": exc.user_message,
            }
        else:
            flash(exc.user_message, "danger")
    except BackendError as exc:
        if deleted:
            flash(f"Deleted {deleted} transcript{'s' if deleted != 1 else ''} before an error occurred.", "warning")
        flash(exc.user_message, "danger")

    return redirect(url_for("ingestion.list_transcripts", corpus_id=corpus_id))


def _flatten_theme_tree(tree: list[dict]) -> dict:
    """Recursively flatten a theme tree into {theme_id_str: {label, description}}."""
    result: dict[str, dict] = {}

    def _walk(nodes: list[dict]) -> None:
        for node in nodes:
            t = node.get("theme", {})
            tid = str(t.get("id", ""))
            if tid:
                result[tid] = {
                    "label": t.get("label", ""),
                    "description": t.get("description") or "",
                }
            _walk(node.get("children", []))

    _walk(tree)
    return result


@bp.get("/<corpus_id>/<document_id>/read")
def read_transcript(corpus_id: str, document_id: str) -> str:
    set_active_corpus_id(corpus_id)
    run_id = request.args.get("run_id", "").strip()

    try:
        client = _backend()
        document = client.get_document_content(corpus_id, document_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("ingestion.list_transcripts", corpus_id=corpus_id))

    # Build dropdown: all succeeded runs across all codebooks for this corpus.
    # Failures here are non-fatal — the transcript still renders without the panel.
    available_runs: list[dict] = []
    try:
        codebooks = client.list_codebooks(corpus_id)
        for cb in codebooks:
            runs = client.list_codebook_application_runs(cb["id"])
            for r in runs:
                if r.get("status") == "succeeded":
                    available_runs.append({
                        "run_id": str(r["id"]),
                        "codebook_id": str(cb["id"]),
                        "codebook_name": cb.get("name", ""),
                        "run_name": r.get("name") or r.get("custom_id") or "",
                        "run_date": (r.get("created_at") or "")[:10],
                    })
        available_runs.sort(key=lambda x: x["run_date"], reverse=True)
    except BackendError:
        pass

    # Fetch document coding and theme metadata for the selected run.
    themes: dict[str, dict] = {}
    code_assignments: list[dict] = []
    if run_id:
        try:
            doc_codings = client.get_codebook_application_run_documents(run_id)
            doc_coding = next(
                (dc for dc in doc_codings if str(dc.get("document_id", "")) == document_id),
                None,
            )
            if doc_coding:
                code_assignments = [
                    ca for ca in doc_coding.get("code_assignments", [])
                    if ca.get("quote")
                    and ca.get("quote_match_status") in ("exact", "normalized", "fuzzy")
                ]
                codebook_id = str(doc_coding.get("codebook_id", ""))
                if codebook_id:
                    tree = client.get_theme_tree(codebook_id)
                    themes = _flatten_theme_tree(tree)
        except BackendError:
            pass

    return render_template(
        "ingestion/read.html",
        document=document,
        corpus_id=corpus_id,
        available_runs=available_runs,
        selected_run_id=run_id,
        themes=themes,
        code_assignments=code_assignments,
    )
