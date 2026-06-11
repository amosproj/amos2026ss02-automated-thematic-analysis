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
        
    if not active_corpus_id:
        disabled_reason = "No active corpus selected."
    else:
        try:
            docs_resp = client.list_documents(active_corpus_id, page_size=1)
            transcripts_count = len(docs_resp)
            
            codebooks = client.list_codebooks(active_corpus_id)
            if not codebooks:
                disabled_reason = "Please configure a codebook for this corpus."
            else:
                codebook = codebooks[0]
                
                if transcripts_count == 0:
                    disabled_reason = "Corpus has no transcripts."
                else:
                    can_run_analysis = True
        except BackendError as exc:
            disabled_reason = f"Error fetching prerequisites: {exc.user_message}"
            
    return render_template(
        "analysis/index.html",
        can_run_analysis=can_run_analysis,
        disabled_reason=disabled_reason,
        codebook=codebook,
        transcripts_count=transcripts_count,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options if active_corpus_id else [],
        switch_template_url=url_for("analysis.index", corpus_id="__CORPUS_ID__"),
        helper_text="Select a corpus to analyze.",
    )

@bp.post("/trigger")
def trigger_analysis():
    corpus_id = request.form.get("corpus_id")
    codebook_id = request.form.get("codebook_id")
    
    if not corpus_id or not codebook_id:
        flash("Missing corpus or codebook ID.", "danger")
        return redirect(url_for("analysis.index"))
        
    client = _backend()
    try:
        job = client.trigger_analysis(corpus_id, codebook_id)
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
