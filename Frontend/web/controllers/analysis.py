from flask import Blueprint, flash, jsonify, redirect, render_template, request, url_for

from web.services.corpus_context import resolve_active_corpus
from web.services.backend_client import BackendError, get_backend_client as _backend

bp = Blueprint("analysis", __name__)


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
    client = _backend()
    try:
        job = client.get_analysis_job(job_id)
        return jsonify(job)
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 500
