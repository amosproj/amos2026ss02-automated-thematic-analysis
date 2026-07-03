import io
import time
import zipfile
from uuid import uuid4

from flask import Blueprint, Response, flash, jsonify, redirect, render_template, request, url_for

from web.services.corpus_context import resolve_active_corpus
from web.services.backend_client import (
    BackendError,
    BackendNotFoundError,
    get_backend_client as _backend,
)

bp = Blueprint("analysis", __name__)

EXPORT_FORMATS = ("theme-based", "participant-based")


@bp.get("/")
def index() -> str:
    client = _backend()
    can_run_analysis = False
    disabled_reason = ""
    codebook = None
    transcripts_count = 0
    active_corpus_id = None
    
    requested_corpus_id = request.args.get("corpus_id")
    try:
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            client, requested_corpus_id=requested_corpus_id
        )
    except BackendError:
        active_corpus_id = None
        
    transcripts = []
    codebooks = []
    previous_runs = []

    # Best-effort: show which LLM provider a run will use. Non-fatal if it fails.
    active_provider_label = None
    try:
        provider_state = client.get_llm_provider()
        active_provider_label = next(
            (opt["label"] for opt in provider_state.get("available", [])
             if opt["id"] == provider_state.get("active")),
            provider_state.get("active"),
        )
    except BackendError:
        active_provider_label = None

    if not active_corpus_id:
        disabled_reason = "No active corpus selected."
    else:
        try:
            # Fetch all documents to populate the dropdown
            docs_resp = client.list_documents(active_corpus_id, page_size=1000)
            transcripts = docs_resp
            transcripts_count = len(transcripts)
            
            codebooks = client.list_codebooks(active_corpus_id)
            if not codebooks:
                disabled_reason = "Please configure a codebook for this corpus."
            else:
                codebook = codebooks[0] # Default selection
                
                if transcripts_count == 0:
                    disabled_reason = "Corpus has no transcripts."
                else:
                    can_run_analysis = True
                
                # Fetch runs for all codebooks
                for cb in codebooks:
                    runs = client.list_codebook_application_runs(cb["id"])
                    # Attach codebook name to each run for display
                    for r in runs:
                        r["codebook_name"] = cb["name"]
                    previous_runs.extend(runs)
                
                # Sort runs by created_at descending
                previous_runs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
                
        except BackendError as exc:
            disabled_reason = f"Error fetching prerequisites: {exc.user_message}"
            
    return render_template(
        "analysis/index.html",
        can_run_analysis=can_run_analysis,
        disabled_reason=disabled_reason,
        codebook=codebook,
        codebooks=codebooks,
        transcripts=transcripts,
        transcripts_count=transcripts_count,
        previous_runs=previous_runs,
        active_corpus_id=active_corpus_id,
        corpus_options=corpus_options if active_corpus_id else [],
        active_provider_label=active_provider_label,
    )

@bp.post("/trigger")
def trigger_analysis():
    corpus_id = request.form.get("corpus_id")
    codebook_id = request.form.get("codebook_id")
    name = request.form.get("name")
    custom_id = request.form.get("custom_id")
    transcript_ids = request.form.getlist("transcript_document_ids")
    
    if not corpus_id or not codebook_id:
        flash("Missing corpus or codebook ID.", "danger")
        return redirect(url_for("analysis.index"))
        
    client = _backend()
    try:
        job = client.trigger_analysis(
            corpus_id=corpus_id, 
            codebook_id=codebook_id,
            name=name,
            custom_id=custom_id,
            transcript_document_ids=transcript_ids if transcript_ids else None
        )
        return redirect(url_for("analysis.wait", job_id=job["id"]))
    except BackendError as exc:
        flash(f"Failed to trigger analysis: {exc.user_message}", "danger")
        return redirect(url_for("analysis.index"))

@bp.get("/job/<job_id>")
def wait(job_id: str) -> str:
    return render_template("analysis/wait.html", job_id=job_id)

@bp.get("/job/<job_id>/status")
def job_status(job_id: str):
    if job_id.startswith(_DEMO_ANALYSIS_PREFIX):
        return jsonify(_demo_analysis_state(job_id))
    client = _backend()
    try:
        job = client.get_analysis_job(job_id)
        return jsonify(job)
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 500


@bp.post("/job/<job_id>/cancel")
def cancel_job(job_id: str):
    entry = _DEMO_ANALYSIS_JOBS.get(job_id)
    if entry is not None:
        entry["cancelled"] = True
        return jsonify({"id": job_id, "status": "cancelled", "phase": "cancelled"})
    try:
        job = _backend().cancel_analysis_job(job_id)
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 400
    return jsonify(job)


# Demo flow: scripted /analysis/demo run (no backend) 
_DEMO_ANALYSIS_PREFIX = "demo-analysis-"
_DEMO_ANALYSIS_DOCS = 10
_DEMO_ANALYSIS_PHASES = [
    (1.0, "queued"), (2.5, "loading_codebook"),
    (9.0, "coding_documents"), (10.5, "persisting"),
]
_DEMO_ANALYSIS_JOBS: dict[str, dict] = {}


def _demo_analysis_state(job_id: str) -> dict:
    entry = _DEMO_ANALYSIS_JOBS.get(job_id)
    if entry is None:
        return {"error": "Demo run expired — start a new one from the Analysis page."}
    if entry.get("cancelled"):
        return {"id": job_id, "status": "cancelled", "phase": "cancelled"}
    elapsed = time.monotonic() - entry["started_at"]
    total = _DEMO_ANALYSIS_DOCS
    phase = next((name for end, name in _DEMO_ANALYSIS_PHASES if elapsed < end), "succeeded")

    if phase == "queued":
        return {"id": job_id, "status": "queued", "phase": "queued",
                "progress_percent": 0, "documents_total": 0, "documents_done": 0}

    # documents_done ramps through coding; one transcript fails so both cards stream.
    ramp = max(0.0, min(1.0, (elapsed - 2.5) / 6.5))
    done = total if phase in ("persisting", "succeeded") else round(ramp * total)
    failed = 1 if done >= 7 else 0
    terminal = phase == "succeeded"
    return {
        "id": job_id, "status": "succeeded" if terminal else "running", "phase": phase,
        "progress_percent": 100 if terminal else max(2, min(99, round(done / total * 99))),
        "documents_total": total, "documents_done": done,
        "documents_coded": done - failed, "documents_failed": failed,
    }


@bp.get("/demo")
def analysis_demo():
    """Play a scripted analysis-progress run without touching the backend."""
    job_id = f"{_DEMO_ANALYSIS_PREFIX}{uuid4().hex}"
    _DEMO_ANALYSIS_JOBS[job_id] = {"started_at": time.monotonic()}
    return redirect(url_for("analysis.wait", job_id=job_id))

@bp.post("/runs/export")
def export_selected_runs() -> Response | str:
    """Export the selected runs as one ZIP with a CSV per run, per chosen format."""
    corpus_id = request.args.get("corpus_id")
    run_ids = [item_id for item_id in request.form.getlist("item_ids") if item_id]
    formats = [fmt for fmt in request.form.getlist("formats") if fmt in EXPORT_FORMATS]
    if not run_ids:
        flash("Select at least one analysis run to export.", "warning")
        return redirect(url_for("analysis.index", corpus_id=corpus_id))
    if not formats:
        flash("Select at least one export format.", "warning")
        return redirect(url_for("analysis.index", corpus_id=corpus_id))

    client = _backend()
    try:
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for run_id in run_ids:
                for export_format in formats:
                    csv_bytes = client.fetch_run_export_csv(run_id, export_format)
                    archive.writestr(f"run-{run_id}-{export_format}.csv", csv_bytes)
        archive_buffer.seek(0)
        return Response(
            archive_buffer.getvalue(),
            mimetype="application/zip",
            headers={"Content-Disposition": 'attachment; filename="analysis-runs-export.zip"'},
        )
    except BackendNotFoundError:
        flash("One of the selected runs couldn't be found. It may have been deleted.", "danger")
        return redirect(url_for("analysis.index", corpus_id=corpus_id))
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("analysis.index", corpus_id=corpus_id))


@bp.post("/runs/delete")
def delete_selected_runs():
    """Hard-delete the analysis runs selected in the Previous Analysis Runs box."""
    corpus_id = request.args.get("corpus_id")
    run_ids = [item_id for item_id in request.form.getlist("item_ids") if item_id]
    if not run_ids:
        flash("Select at least one analysis run to delete.", "warning")
        return redirect(url_for("analysis.index", corpus_id=corpus_id))

    client = _backend()
    deleted = 0
    try:
        for run_id in run_ids:
            client.delete_codebook_application_run(run_id)
            deleted += 1
        flash(f"Deleted {deleted} analysis run{'s' if deleted != 1 else ''}.", "success")
    except BackendError as exc:
        if deleted:
            flash(
                f"Deleted {deleted} analysis run{'s' if deleted != 1 else ''} before an error occurred.",
                "warning",
            )
        flash(exc.user_message, "danger")

    return redirect(url_for("analysis.index", corpus_id=corpus_id))
