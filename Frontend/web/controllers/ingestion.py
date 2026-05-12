from flask import Blueprint, current_app, render_template, request

from web.services.backend_client import BackendClient, BackendError

bp = Blueprint("ingestion", __name__)


def _backend() -> BackendClient:
    return current_app.extensions["backend_client"]


def _resolve_workspace_corpus(client: BackendClient) -> str:
    """MVP single-workspace: every request shares one corpus, created on demand."""
    cfg = current_app.config
    return client.ensure_corpus(
        project_id=cfg["DEFAULT_PROJECT_ID"],
        name=cfg["DEFAULT_CORPUS_NAME"],
    )


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------


def _render_upload_form(error: str | None = None) -> str:
    return render_template(
        "ingestion/upload.html",
        max_size_mb=current_app.config["MAX_UPLOAD_SIZE_MB"],
        accepted_extensions=sorted(current_app.config["ACCEPTED_EXTENSIONS"]),
        error=error,
    )


def _file_size(fileobj) -> int:
    stream = fileobj.stream
    pos = stream.tell()
    stream.seek(0, 2)
    size = stream.tell()
    stream.seek(pos)
    return size


@bp.get("/upload")
def upload_form() -> str:
    return _render_upload_form()


@bp.post("/upload")
def upload_submit() -> str:
    files = [f for f in request.files.getlist("files") if f and f.filename]
    if not files:
        return _render_upload_form(error="Please select at least one file to upload.")

    max_bytes = current_app.config["MAX_UPLOAD_SIZE_MB"] * 1024 * 1024
    oversize = [f.filename for f in files if _file_size(f) > max_bytes]
    if oversize:
        max_mb = current_app.config["MAX_UPLOAD_SIZE_MB"]
        return _render_upload_form(
            error=f"Each file must be at most {max_mb} MB. Too large: {', '.join(oversize)}."
        )

    try:
        client = _backend()
        corpus_id = _resolve_workspace_corpus(client)
        results = client.upload_files(corpus_id, files)
    except BackendError as exc:
        return render_template("ingestion/results.html", results=[], error=str(exc))

    return render_template("ingestion/results.html", results=results, error=None)


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


@bp.get("/")
def list_transcripts() -> str:
    try:
        client = _backend()
        corpus_id = _resolve_workspace_corpus(client)
        documents = client.list_documents(corpus_id)
    except BackendError as exc:
        return render_template("ingestion/list.html", documents=[], error=str(exc))
    return render_template("ingestion/list.html", documents=documents, error=None)
