from flask import Blueprint, current_app, flash, redirect, render_template, request, url_for

from web.services.backend_client import BackendClient, BackendError

bp = Blueprint("ingestion", __name__)


def _backend() -> BackendClient:
    return current_app.extensions["backend_client"]


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


# ---------------------------------------------------------------------------
# Landings — resolve the default corpus then redirect to the corpus-scoped URL.
# ---------------------------------------------------------------------------


@bp.get("/")
def transcripts_landing():
    corpus_id = _resolve_workspace_corpus(_backend())
    return redirect(url_for("ingestion.list_transcripts", corpus_id=corpus_id))


@bp.get("/upload")
def upload_landing():
    corpus_id = _resolve_workspace_corpus(_backend())
    return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))


# ---------------------------------------------------------------------------
# Upload (corpus-scoped)
# ---------------------------------------------------------------------------


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

    max_bytes = current_app.config["MAX_UPLOAD_SIZE_MB"] * 1024 * 1024
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
    except BackendError as exc:
        flash(str(exc), "danger")
        return render_template("ingestion/results.html", results=[], corpus_id=corpus_id)

    return render_template("ingestion/results.html", results=results, corpus_id=corpus_id)


# ---------------------------------------------------------------------------
# List (corpus-scoped)
# ---------------------------------------------------------------------------


@bp.get("/<corpus_id>/")
def list_transcripts(corpus_id: str) -> str:
    try:
        documents = _backend().list_documents(corpus_id)
    except BackendError as exc:
        flash(str(exc), "danger")
        return render_template("ingestion/list.html", documents=[], corpus_id=corpus_id)
    return render_template("ingestion/list.html", documents=documents, corpus_id=corpus_id)
