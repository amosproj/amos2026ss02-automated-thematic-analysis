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
    return render_template(
        "ingestion/list.html",
        documents=documents,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        active_corpus_name=active_corpus_name,
    )
