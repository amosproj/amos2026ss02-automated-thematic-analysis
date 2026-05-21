from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from web.services.backend_client import (
    BackendClient,
    BackendError,
    BackendValidationError,
    get_backend_client as _backend,
)

bp = Blueprint("ingestion", __name__)


def _resolve_workspace_corpus(client: BackendClient) -> str:
    """MVP single-workspace: resolve or create the default corpus.

    Only invoked from the no-arg landing routes. Every other view receives
    the corpus_id from the URL, so the id is explicit and shareable rather
    than hidden in an in-memory cache."""
    cfg = current_app.config
    return client.ensure_corpus(
        project_id=cfg["DEFAULT_PROJECT_ID"],
        name=cfg["DEFAULT_CORPUS_NAME"],
    )


# Landings — resolve the default corpus then redirect to the corpus-scoped URL.


def _landing_with_corpus(target_endpoint: str):
    """Both landings share the same shape: resolve the default corpus, then
    redirect to a corpus-scoped view. On backend failure, render the empty
    transcript list with the user-facing error message — the only sensible
    fallback view when we don't yet know a corpus id."""
    try:
        corpus_id = _resolve_workspace_corpus(_backend())
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
    return render_template(
        "ingestion/upload.html",
        corpus_id=corpus_id,
        max_size_mb=current_app.config["MAX_UPLOAD_SIZE_MB"],
        accepted_extensions=sorted(current_app.config["ACCEPTED_EXTENSIONS"]),
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
def upload_submit(corpus_id: str) -> str:
    files = [f for f in request.files.getlist("files") if f and f.filename]
    if not files:
        flash("Please select at least one file to upload.", "danger")
        return _render_upload_form(corpus_id)

    max_bytes = current_app.config["MAX_UPLOAD_BYTES"]
    oversize = [f.filename for f in files if _file_size(f) > max_bytes]
    if oversize:
        max_mb = current_app.config["MAX_UPLOAD_SIZE_MB"]
        flash(
            f"Each file must be at most {max_mb} MB. Too large: {', '.join(oversize)}.",
            "danger",
        )
        return _render_upload_form(corpus_id)

    try:
        results = _backend().upload_files(corpus_id, files)
    except BackendValidationError as exc:
        # Backend rejected the payload — re-render the form so the user
        # can fix it. The validation message names the offending field.
        flash(exc.user_message, "danger")
        return _render_upload_form(corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "ingestion/results.html", results=[], corpus_id=corpus_id, error=True
        )

    return render_template("ingestion/results.html", results=results, corpus_id=corpus_id)

# List (corpus-scoped)



@bp.get("/<corpus_id>/")
def list_transcripts(corpus_id: str) -> str:
    try:
        documents = _backend().list_documents(corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "ingestion/list.html", documents=[], corpus_id=corpus_id, error=True
        )
    return render_template("ingestion/list.html", documents=documents, corpus_id=corpus_id)
