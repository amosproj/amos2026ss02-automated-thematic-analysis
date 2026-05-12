from flask import Blueprint, current_app, render_template

bp = Blueprint("ingestion", __name__)


@bp.get("/upload")
def upload_form() -> str:
    return render_template(
        "ingestion/upload.html",
        max_size_mb=current_app.config["MAX_UPLOAD_SIZE_MB"],
        accepted_extensions=sorted(current_app.config["ACCEPTED_EXTENSIONS"]),
    )


@bp.post("/upload")
def upload_submit():
    # TODO: validate files (extension, size, dedupe filename), then forward each
    # to POST {BACKEND_API_URL}/ingestion/corpora/{corpus_id}/upload via httpx
    # and surface per-file success/error messages.
    raise NotImplementedError("Transcript upload handler not implemented yet.")


@bp.get("/")
def list_transcripts() -> str:
    # TODO: fetch GET {BACKEND_API_URL}/ingestion/corpora/{corpus_id}/documents
    # and render the list with original filenames.
    return render_template("ingestion/list.html", transcripts=[])
