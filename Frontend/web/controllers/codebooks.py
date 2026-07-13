import csv
import io
import time
import zipfile
from urllib.parse import quote_plus

from flask import (
    Blueprint,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from web.services.backend_client import (
    BackendConflictError,
    BackendError,
    BackendNotFoundError,
    get_backend_client as _backend,
)
from web.services.corpus_context import resolve_active_corpus, set_active_corpus_id

bp = Blueprint("codebooks", __name__)

CODING_MODES = ("auto", "semi", "manual")

_QUERY_MIN = 10
_QUERY_MAX = 500
_PENDING_CODEBOOK_DELETE_KEY = "pending_codebook_delete"

_MAX_REFINEMENT_ROUNDS_DEFAULT = 5
_MAX_REFINEMENT_ROUNDS_MAX = 10


def _format_run_timestamp(run: dict) -> str:
    raw = run.get("finished_at") or run.get("created_at") or ""
    return str(raw).replace("T", " ")[:16]


def _prepare_application_runs(
    application_runs: list[dict],
    requested_run_id: str,
) -> tuple[list[dict], dict | None, str]:
    decorated_runs = []
    latest_run = max(
        application_runs,
        key=lambda run: str(run.get("finished_at") or run.get("created_at") or ""),
        default=None,
    )
    latest_successful_run = max(
        (run for run in application_runs if run.get("status") == "succeeded"),
        key=lambda run: str(run.get("finished_at") or run.get("created_at") or ""),
        default=None,
    )
    latest_successful_id = str(latest_successful_run.get("id")) if latest_successful_run else ""
    fallback_run_id = latest_successful_id or (str(latest_run.get("id")) if latest_run else "")

    valid_run_ids = {str(run.get("id")) for run in application_runs}
    selected_run_id = requested_run_id if requested_run_id in valid_run_ids else ""
    if not selected_run_id:
        selected_run_id = fallback_run_id

    for run in application_runs:
        run_id = str(run.get("id"))
        timestamp = _format_run_timestamp(run)
        decorated_runs.append({
            **run,
            "id": run_id,
            "timestamp_label": timestamp,
            "is_latest_successful": run_id == latest_successful_id,
        })

    selected_run = next(
        (run for run in decorated_runs if str(run.get("id")) == selected_run_id),
        None,
    )
    return decorated_runs, selected_run, selected_run_id


def _safe_export_filename(name: str, version: int | str | None) -> str:
    safe_name = "".join(
        ch if ch.isalnum() or ch in ("-", "_") else "_"
        for ch in (name or "codebook").strip()
    ).strip("_")
    if not safe_name:
        safe_name = "codebook"
    return f"{safe_name}_v{version or 1}.csv"


def _codebook_to_csv(codebook: dict) -> str:
    themes = codebook.get("themes", [])
    codes = codebook.get("codes", [])

    flat_rows = []
    exported_ids: set = set()

    def traverse(node: dict, parent_name: str) -> None:
        exported_ids.add(node.get("id"))
        flat_rows.append({
            "Node Type": node.get("node_type", "THEME"),
            "Name": node.get("name", ""),
            "Description": node.get("description", ""),
            "Parent Name": parent_name,
        })
        for child in node.get("children", []):
            traverse(child, node.get("name", ""))

    for theme in themes:
        traverse(theme, "")

    for code in codes:
        if code.get("id") not in exported_ids:
            flat_rows.append({
                "Node Type": "CODE",
                "Name": code.get("name", ""),
                "Description": code.get("description", ""),
                "Parent Name": "",
            })

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["Node Type", "Name", "Description", "Parent Name"])
    writer.writeheader()
    writer.writerows(flat_rows)
    return output.getvalue()


def _pending_codebook_delete(corpus_id: str) -> dict | None:
    pending = session.pop(_PENDING_CODEBOOK_DELETE_KEY, None)
    if not isinstance(pending, dict) or pending.get("corpus_id") != corpus_id:
        return None
    item_ids = [item_id for item_id in pending.get("item_ids", []) if item_id]
    if not item_ids:
        return None
    return {
        "message": pending.get("message") or "Deleting these codebooks would interrupt a running analysis.",
        "item_ids": item_ids,
        "action": url_for("codebooks.delete_selected_codebooks", corpus_id=corpus_id),
        "title": "Delete Codebooks",
        "confirm_label": "Delete",
    }


@bp.get("/")
def list_codebooks() -> str:
    requested_corpus_id = request.args.get("corpus_id")
    try:
        active_corpus_id, _, _ = resolve_active_corpus(
            _backend(),
            requested_corpus_id=requested_corpus_id,
            strict_requested=bool(requested_corpus_id),
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "codebooks/list.html",
            codebooks=[],
            corpus_id=None,
            corpus_options=[],
            active_corpus_name=None,
            error=True,
        )

    return redirect(url_for("codebooks.list_codebooks_for_corpus", corpus_id=active_corpus_id))


@bp.get("/upload")
def codebooks_upload_landing() -> str:
    requested_corpus_id = request.args.get("corpus_id")
    try:
        active_corpus_id, _, _ = resolve_active_corpus(
            _backend(),
            requested_corpus_id=requested_corpus_id,
            strict_requested=bool(requested_corpus_id),
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("codebooks.list_codebooks"))

    return redirect(url_for("ingestion.upload_form", corpus_id=active_corpus_id, focus="codebook"))


@bp.get("/<corpus_id>/")
def list_codebooks_for_corpus(corpus_id: str) -> str:
    set_active_corpus_id(corpus_id)
    corpus_name = "Selected Corpus"
    corpus_options: list[dict] = [{"id": corpus_id, "name": corpus_name}]
    try:
        client = _backend()
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            client,
            requested_corpus_id=corpus_id,
            strict_requested=True,
        )
        corpus_name = active_corpus.get("name", corpus_name)
        codebooks = client.list_codebooks(corpus_id=active_corpus_id)
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "codebooks/list.html",
            codebooks=[],
            corpus_id=corpus_id,
            corpus_options=corpus_options,
            active_corpus_name=corpus_name,
            running_jobs=[],
            pending_analysis_delete=None,
            error=True,
        )

    # Fetch in-progress runs so they show in any session, not just the one
    # that started them. Failures are non-fatal; the client-side tracker covers.
    running_jobs: list[dict] = []
    try:
        running_jobs = client.list_generation_jobs(
            corpus_id=active_corpus_id, statuses=["queued", "running"]
        )
    except BackendError:
        running_jobs = []

    return render_template(
        "codebooks/list.html",
        codebooks=codebooks,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        active_corpus_name=corpus_name,
        running_jobs=running_jobs,
        pending_analysis_delete=_pending_codebook_delete(active_corpus_id),
    )


@bp.get("/<codebook_id>/themes")
def codebook_themes(codebook_id: str) -> str:
    requested_corpus_id = request.args.get("corpus_id")
    if requested_corpus_id:
        return redirect(
            url_for(
                "codebooks.codebook_themes_for_corpus",
                corpus_id=requested_corpus_id,
                codebook_id=codebook_id,
                name=request.args.get("name", ""),
                version=request.args.get("version", ""),
                application_run_id=request.args.get("application_run_id", ""),
            )
        )

    # Backward-compatible route: use whichever corpus is active in session.
    try:
        active_corpus_id, _, _ = resolve_active_corpus(_backend())
    except BackendError as exc:
        flash(exc.user_message, "danger")
        active_corpus_id = ""
    if active_corpus_id:
        return redirect(
            url_for(
                "codebooks.codebook_themes_for_corpus",
                corpus_id=active_corpus_id,
                codebook_id=codebook_id,
                name=request.args.get("name", ""),
                version=request.args.get("version", ""),
                application_run_id=request.args.get("application_run_id", ""),
            )
        )
    return redirect(url_for("codebooks.list_codebooks"))


@bp.get("/<corpus_id>/<codebook_id>/themes")
def codebook_themes_for_corpus(corpus_id: str, codebook_id: str) -> str:
    set_active_corpus_id(corpus_id)
    name = request.args.get("name", "")
    version = request.args.get("version", "")
    selected_application_run_id = request.args.get("application_run_id", "")
    active_codebook_id = codebook_id
    selected_application_run: dict | None = None
    corpus_name = "Selected Corpus"
    corpus_options: list[dict] = [{"id": corpus_id, "name": corpus_name}]
    try:
        client = _backend()
        active_corpus_id, corpus_options, active_corpus = resolve_active_corpus(
            client,
            requested_corpus_id=corpus_id,
            strict_requested=True,
        )
        corpus_name = active_corpus.get("name", corpus_name)
        codebooks = client.list_codebooks(corpus_id=active_corpus_id)
        active_codebook = next(
            (cb for cb in codebooks if str(cb.get("id")) == str(codebook_id)),
            None,
        )
        if active_codebook is None:
            raise BackendNotFoundError(
                user_message=(
                    "That codebook couldn't be found in the selected corpus. "
                    "Please choose another codebook."
                )
            )

        active_codebook_id = str(active_codebook["id"])
        if not name:
            name = active_codebook.get("name", "")
        if not version and active_codebook.get("version") is not None:
            version = str(active_codebook["version"])
        research_query = active_codebook.get("research_query") or ""
        researcher_topics = active_codebook.get("researcher_topics") or ""
        application_runs = client.list_codebook_application_runs(active_codebook_id)
        _, selected_application_run, selected_application_run_id = (
            _prepare_application_runs(application_runs, selected_application_run_id)
        )

        frequencies = client.get_theme_frequencies(
            active_codebook_id,
            application_run_id=selected_application_run_id or None,
        )
        tree = client.get_theme_tree(active_codebook_id)
        codebook = client.get_codebook(active_codebook_id)
        codes = codebook.get("codes", [])

        # Best-effort: demographic variables available for per-theme breakdowns.
        # A failure here must not block the themes page, so default to none.
        try:
            demographic_dimensions = client.get_demographic_dimensions(active_corpus_id)
        except BackendError:
            demographic_dimensions = []
    except BackendNotFoundError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "codebooks/themes.html",
            codebook_id=active_codebook_id,
            name=name,
            version=version,
            corpus_id=corpus_id,
            corpus_options=corpus_options,
            active_corpus_name=corpus_name,
            frequencies=[],
            tree=[],
            codes=[],
            research_query="",
            researcher_topics="",
            selected_application_run_id=selected_application_run_id,
            selected_application_run=selected_application_run,
            demographic_dimensions=[],
            error=True,
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "codebooks/themes.html",
            codebook_id=active_codebook_id,
            name=name,
            version=version,
            corpus_id=corpus_id,
            corpus_options=corpus_options,
            active_corpus_name=corpus_name,
            frequencies=[],
            tree=[],
            codes=[],
            research_query="",
            researcher_topics="",
            selected_application_run_id=selected_application_run_id,
            selected_application_run=selected_application_run,
            demographic_dimensions=[],
            error=True,
        )
    return render_template(
        "codebooks/themes.html",
        codebook_id=active_codebook_id,
        name=name,
        version=version,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        active_corpus_name=corpus_name,
        frequencies=frequencies,
        tree=tree,
        codes=codes,
        research_query=research_query,
        researcher_topics=researcher_topics,
        selected_application_run_id=selected_application_run_id,
        selected_application_run=selected_application_run,
        demographic_dimensions=demographic_dimensions,
    )

@bp.get("/<corpus_id>/<codebook_id>/themes/<theme_id>/demographic-breakdown.json")
def theme_demographic_breakdown_json(corpus_id: str, codebook_id: str, theme_id: str):
    dimensions = [d for d in request.args.get("dimensions", "").split(",") if d]
    application_run_id = request.args.get("application_run_id") or None
    try:
        result = _backend().get_theme_demographic_breakdown(
            codebook_id,
            theme_id,
            dimensions,
            application_run_id=application_run_id,
        )
        return jsonify(result)
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 502
    except Exception:
        return jsonify({"error": "An unexpected error occurred."}), 500


@bp.get("/<corpus_id>/<codebook_id>/themes/<theme_id>/quotes.json")
def theme_quotes_json(corpus_id: str, codebook_id: str, theme_id: str):
    try:
        page = max(1, int(request.args.get("page", 1)))
        page_size = max(1, min(100, int(request.args.get("page_size", 20))))
    except (TypeError, ValueError):
        page, page_size = 1, 20

    include_descendants = request.args.get("include_descendants", "true").lower() != "false"
    try:
        application_run_id = request.args.get("application_run_id") or None
        result = _backend().get_theme_quotes(
            codebook_id,
            theme_id,
            page,
            page_size,
            application_run_id=application_run_id,
            include_descendants=include_descendants,
        )
        return jsonify(result)
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 502
    except Exception:
        return jsonify({"error": "An unexpected error occurred."}), 500


@bp.get("/<corpus_id>/<codebook_id>/export")
def export_codebook(corpus_id: str, codebook_id: str) -> Response | str:
    """Export a codebook and its hierarchical themes as a CSV file."""
    try:
        client = _backend()
        codebook = client.get_codebook(codebook_id)
        csv_data = _codebook_to_csv(codebook)
        filename = _safe_export_filename(codebook.get("name", "codebook"), codebook.get("version", 1))

        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except BackendNotFoundError:
        flash("That codebook couldn't be found. It may have been deleted.", "danger")
        return redirect(url_for("codebooks.list_codebooks", corpus_id=corpus_id))
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("codebooks.list_codebooks", corpus_id=corpus_id))


@bp.post("/<corpus_id>/export")
def export_selected_codebooks(corpus_id: str) -> Response | str:
    """Export selected codebooks as one ZIP containing one CSV per codebook."""
    codebook_ids = [item_id for item_id in request.form.getlist("item_ids") if item_id]
    if not codebook_ids:
        flash("Select at least one codebook to export.", "warning")
        return redirect(url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id))

    try:
        client = _backend()
        archive_buffer = io.BytesIO()
        used_filenames: set[str] = set()

        with zipfile.ZipFile(archive_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for codebook_id in codebook_ids:
                codebook = client.get_codebook(codebook_id)
                filename = _safe_export_filename(
                    codebook.get("name", "codebook"),
                    codebook.get("version", 1),
                )
                if filename in used_filenames:
                    stem, ext = filename.rsplit(".", 1)
                    filename = f"{stem}_{codebook_id}.{ext}"
                used_filenames.add(filename)
                archive.writestr(filename, _codebook_to_csv(codebook))

        archive_buffer.seek(0)
        return Response(
            archive_buffer.getvalue(),
            mimetype="application/zip",
            headers={"Content-Disposition": "attachment; filename=selected_codebooks.zip"},
        )
    except BackendNotFoundError:
        flash("One of the selected codebooks couldn't be found. It may have been deleted.", "danger")
        return redirect(url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id))
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id))


@bp.post("/<corpus_id>/upload")
def upload_submit(corpus_id: str) -> str:
    """Handle a CSV codebook upload from the unified upload page."""
    file = request.files.get("file")
    if not file or not file.filename:
        flash("Please select a CSV file to upload.", "danger")
        return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))

    if not file.filename.lower().endswith(".csv"):
        flash("Only CSV files (.csv extension) are supported.", "danger")
        return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))

    try:
        client = _backend()
        parsed_themes = client.parse_csv_preview(file)
        # Derive a readable default name from the file name
        default_name = file.filename.rsplit(".", 1)[0].replace("_", " ").title()
        return render_template(
            "codebooks/review.html",
            corpus_id=corpus_id,
            codebook_id=None,
            codebook_name=default_name,
            nodes=_themes_to_review_nodes(parsed_themes),
            error=None,
        )
    except BackendError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))

@bp.get("/<corpus_id>/manual")
def manual_form(corpus_id: str) -> str:
    """Render the review editor pre-filled with one blank row for manual entry."""
    blank_nodes = [{"name": "", "description": "", "indent": 0, "is_code": False}]
    return render_template(
        "codebooks/review.html",
        corpus_id=corpus_id,
        codebook_id=None,
        codebook_name="New Codebook",
        nodes=blank_nodes,
        error=None,
        cancel_url=url_for("ingestion.upload_form", corpus_id=corpus_id, focus="codebook"),
    )

@bp.post("/<corpus_id>/confirm")
def confirm_submit(corpus_id: str) -> str:
    """Validate, customise, and confirm a codebook and its themes."""
    codebook_name = (request.form.get("codebook_name") or "").strip()
    source_codebook_id = request.form.get("source_codebook_id", "").strip()
    themes = _parse_review_rows()

    def _re_render(error: str):
        return render_template(
            "codebooks/review.html",
            corpus_id=corpus_id,
            codebook_id=None,
            codebook_name=codebook_name,
            nodes=_themes_to_review_nodes(themes),
            source_codebook_id=source_codebook_id or None,
            error=error,
        )

    error = _validate_review_themes(codebook_name, themes)
    if error:
        return _re_render(error)

    # If this is an edit of an existing codebook, check whether anything actually
    # changed. If not, skip the create and go straight to the success page.
    if source_codebook_id:
        try:
            original = _backend().get_codebook(source_codebook_id)
            original_name = (original.get("name") or "").strip()
            original_themes = _flatten_codebook_relational(original)
            # Normalise parent_name to "" on both sides before comparing —
            # the form assembles None for empty parents, the flatten helper uses "".
            def _normalise(rows: list[dict]) -> list[dict]:
                return [
                    {**r, "parent_name": r.get("parent_name") or ""}
                    for r in rows
                ]
            if codebook_name == original_name and _normalise(themes) == _normalise(original_themes):
                flash("No changes were made to the codebook.", "info")
                return redirect(url_for(
                    "codebooks.success",
                    corpus_id=corpus_id,
                    codebook_id=source_codebook_id,
                ))
        except BackendError:
            pass  # original no longer accessible; fall through to create

    try:
        client = _backend()
        res = client.create_codebook(corpus_id=corpus_id, name=codebook_name, themes=themes)
        codebook_id = res["id"]
    except BackendError as exc:
        return _re_render(str(exc))

    # Semi-auto: the edited codebook is saved, so delete the original draft to leave just one. 
    # Best-effort — the new codebook already exists.
    if source_codebook_id and source_codebook_id != str(codebook_id):
        try:
            client.delete_codebook(source_codebook_id)
        except BackendError:
            pass

    return redirect(url_for("codebooks.success", corpus_id=corpus_id, codebook_id=codebook_id))

@bp.get("/<corpus_id>/success")
def success(corpus_id: str) -> str:
    """Show details of the successfully saved codebook."""
    codebook_id = request.args.get("codebook_id")
    if not codebook_id:
        return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id, focus="codebook"))

    try:
        client = _backend()
        codebook = client.get_codebook(codebook_id)
        return render_template("codebooks/success.html", corpus_id=corpus_id, codebook=codebook, error=None)
    except BackendError as exc:
        return render_template("codebooks/success.html", corpus_id=corpus_id, codebook=None, error=str(exc))


# Wizard: Create New Codebook ------------------------------------------------


@bp.get("/new")
def new_codebook_landing():
    try:
        active_corpus_id, _, _ = resolve_active_corpus(_backend())
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("codebooks.list_codebooks"))
    return redirect(url_for("codebooks.new_codebook_mode_select", corpus_id=active_corpus_id))


@bp.get("/new/<corpus_id>")
def new_codebook_mode_select(corpus_id: str) -> str:
    set_active_corpus_id(corpus_id)
    selected = request.args.get("mode") or ""
    return render_template(
        "codebooks/new/mode_select.html",
        corpus_id=corpus_id,
        selected=selected if selected in CODING_MODES else "",
    )


@bp.post("/new/<corpus_id>")
def new_codebook_mode_submit(corpus_id: str):
    mode = request.form.get("mode", "")
    if mode not in CODING_MODES:
        flash("Please select a coding mode before continuing.", "danger")
        return render_template(
            "codebooks/new/mode_select.html",
            corpus_id=corpus_id,
            selected="",
        )

    if mode in ("auto", "semi"):
        return redirect(
            url_for("codebooks.new_codebook_auto_form", corpus_id=corpus_id, mode=mode)
        )

    # mode == "manual": send to the unified upload page with the codebook card focused.
    return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id, focus="codebook"))


def _resolve_mode(value: str) -> str:
    return value if value in ("auto", "semi") else "auto"


def _active_provider_label() -> str | None:
    """Best-effort label of the active LLM provider; None if backend is down."""
    try:
        state = _backend().get_llm_provider()
        return next(
            (opt["label"] for opt in state.get("available", [])
             if opt["id"] == state.get("active")),
            state.get("active"),
        )
    except BackendError:
        return None


def _corpus_document_count(corpus_id: str) -> int | None:
    """Best-effort transcript count for the sample-size hint; None if backend is down."""
    try:
        return _backend().count_documents(corpus_id)
    except BackendError:
        return None


@bp.get("/new/<corpus_id>/auto")
def new_codebook_auto_form(corpus_id: str) -> str:
    mode = _resolve_mode(request.args.get("mode", ""))
    return render_template(
        "codebooks/new/auto_form.html",
        corpus_id=corpus_id,
        mode=mode,
        codebook_name=request.args.get("name", ""),
        research_query=request.args.get("rq", ""),
        researcher_topics=request.args.get("rt", ""),
        max_refinement_rounds=request.args.get("mri", str(_MAX_REFINEMENT_ROUNDS_DEFAULT)),
        transcript_sample_size=request.args.get("n", ""),
        max_refinement_rounds_max=_MAX_REFINEMENT_ROUNDS_MAX,
        corpus_document_count=_corpus_document_count(corpus_id),
        active_provider_label=_active_provider_label(),
    )


@bp.post("/new/<corpus_id>/auto")
def new_codebook_auto_submit(corpus_id: str):
    mode = _resolve_mode(request.form.get("mode", ""))
    name = (request.form.get("codebook_name") or "").strip()
    raw_research_query = request.form.get("research_query") or ""
    research_query = raw_research_query.strip()
    researcher_topics = (request.form.get("researcher_topics") or "").strip()
    max_refinement_rounds_raw = request.form.get(
        "max_refinement_rounds", str(_MAX_REFINEMENT_ROUNDS_DEFAULT)
    )
    transcript_sample_size_raw = (request.form.get("transcript_sample_size") or "").strip()

    def _render_form(rq_error: str | None = None, rt_error: str | None = None, ts_error: str | None = None):
        return render_template(
            "codebooks/new/auto_form.html",
            corpus_id=corpus_id,
            mode=mode,
            codebook_name=name,
            research_query=research_query,
            researcher_topics=researcher_topics,
            max_refinement_rounds=max_refinement_rounds_raw,
            transcript_sample_size=transcript_sample_size_raw,
            max_refinement_rounds_max=_MAX_REFINEMENT_ROUNDS_MAX,
            corpus_document_count=_corpus_document_count(corpus_id),
            rq_error=rq_error,
            rt_error=rt_error,
            ts_error=ts_error,
            active_provider_label=_active_provider_label(),
        )

    if not name:
        flash("Please give your codebook a name.", "danger")
        return _render_form()

    # The research question is optional: leaving it blank is fine. But if the
    # researcher actually types something it must be a real question — not just
    # whitespace, and within the length bounds.
    if raw_research_query and not research_query:
        return _render_form(rq_error="Research question cannot be only whitespace.")
    if research_query and len(research_query) < _QUERY_MIN:
        return _render_form(
            rq_error=f"Research question must be at least {_QUERY_MIN} characters."
        )
    if len(research_query) > _QUERY_MAX:
        return _render_form(rq_error=f"Research question must be at most {_QUERY_MAX} characters.")

    # Topics are optional, free-form keywords; only cap their length.
    if len(researcher_topics) > _QUERY_MAX:
        return _render_form(rt_error=f"Topics must be at most {_QUERY_MAX} characters.")

    # The dropdown's value is the max number of iterations shown to the user
    # (matches the live "Iteration N of M" progress display). The backend's
    # max_refinement_rounds field means "rounds after the first" — the
    # generation loop runs max_refinement_rounds + 1 iterations — so translate
    # here rather than exposing that off-by-one to the UI. Always comes from a
    # fixed <select>, so it's always a valid integer in range — no need to
    # defend against malformed input here.
    max_iterations = int(max_refinement_rounds_raw)
    max_refinement_rounds = max(0, max_iterations - 1)

    # transcript_sample_size is free-text, so it does need format validation.
    # Whether it exceeds the corpus size is checked by the backend (the
    # authoritative source for transcript counts), surfaced via the
    # BackendError flash below rather than duplicated here.
    transcript_sample_size: int | None = None
    if transcript_sample_size_raw:
        try:
            transcript_sample_size = int(transcript_sample_size_raw)
        except ValueError:
            return _render_form(ts_error="Number of transcripts must be a whole number.")
        if transcript_sample_size <= 0:
            return _render_form(ts_error="Number of transcripts must be at least 1.")

    try:
        job = _backend().create_generation_job(
            codebook_name=name,
            corpus_id=corpus_id,
            research_query=research_query or None,
            researcher_topics=researcher_topics or None,
            max_refinement_rounds=max_refinement_rounds,
            transcript_sample_size=transcript_sample_size,
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return _render_form()

    encoded_job = quote_plus(str(job["id"]))
    encoded_name = quote_plus(name)

    if mode == "semi":
        # Go straight to the progress page; it opens the review editor on
        # success. Name (not new_job) is passed so the pagehide handler can
        # register a background-tracker fallback if the user navigates away.
        return redirect(
            url_for("codebooks.new_codebook_job_progress", job_id=job["id"])
            + f"?mode=semi&name={encoded_name}"
        )

    # auto: hand off to the codebook list; job_tracker.js picks up new_job
    # and polls in the background while the user navigates freely.
    return redirect(
        url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id)
        + f"?new_job={encoded_job}&mode={mode}&name={encoded_name}"
    )


@bp.get("/new/jobs/<job_id>")
def new_codebook_job_progress(job_id: str) -> str:
    mode = _resolve_mode(request.args.get("mode", ""))
    name = request.args.get("name", "")
    return render_template(
        "codebooks/new/progress.html",
        job_id=job_id,
        mode=mode,
        codebook_name=name,
    )


@bp.get("/new/jobs/<job_id>.json")
def new_codebook_job_status(job_id: str):
    # Errors come back as 200 with `{error: "..."}` so the poller doesn't need
    # to distinguish HTTP failure modes.
    if job_id.startswith(_DEMO_JOB_PREFIX):
        return jsonify(_demo_job_state(job_id))
    try:
        job = _backend().get_generation_job(job_id)
    except BackendNotFoundError as exc:
        return jsonify({"error": exc.user_message, "not_found": True}), 200
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 200
    return jsonify(job)


# Demo flow -----------------------------------------------------------------
#
# Lets you walk the wizard end-to-end without burning LLM tokens. The route
# creates a real codebook via `POST /codebooks/` with a hardcoded payload
# (~6 themes, 4 codes, two-level hierarchy), then runs ~5 seconds of scripted
# progress before the existing progress page redirects to the review editor.
# The editor reads the actual persisted codebook, so the interface behaves
# identically to a real run from there on.

_DEMO_JOB_PREFIX = "demo-"
_DEMO_TIMELINE_SECONDS = 5.0
# Module-level: maps demo_job_id → {"started_at": ts, "codebook_id": str}.
# Per-process and ephemeral by design — restart the frontend to clear.
_DEMO_JOBS: dict[str, dict] = {}


def _demo_codebook_nodes() -> list[dict]:
    """Sample codebook content covering the editor's full surface.

    Six themes with one level of nesting and four flat codes. Enough to
    exercise drag-to-reorder, indent/outdent, the codes section, and the
    backend's NodeInput auto-promotion to SUBTHEME on parent_name."""
    return [
        {"name": "Work-Life Balance",
         "description": "How participants negotiate work and personal life.",
         "parent_name": None},
        {"name": "Boundary Difficulties",
         "description": "Trouble separating work from home time.",
         "parent_name": "Work-Life Balance"},
        {"name": "Flexible Hours",
         "description": "Working outside conventional 9-to-5 hours.",
         "parent_name": "Work-Life Balance"},
        {"name": "Team Collaboration",
         "description": "Working with colleagues across functions.",
         "parent_name": None},
        {"name": "Remote Communication",
         "description": "Tools and habits for distributed teamwork.",
         "parent_name": "Team Collaboration"},
        {"name": "Career Growth",
         "description": "Professional development and skill-building.",
         "parent_name": None},
        {"node_type": "CODE", "name": "Late evenings",
         "description": "Working past 8pm on a regular basis.",
         "parent_name": "Boundary Difficulties"},
        {"node_type": "CODE", "name": "Long meetings",
         "description": "Synchronous calls exceeding one hour.",
         "parent_name": "Remote Communication"},
        {"node_type": "CODE", "name": "Async messaging",
         "description": "Using chat or email for non-urgent updates.",
         "parent_name": "Remote Communication"},
        {"node_type": "CODE", "name": "Skill workshops",
         "description": "Attending or hosting training sessions.",
         "parent_name": "Career Growth"},
    ]


def _demo_job_state(job_id: str) -> dict:
    entry = _DEMO_JOBS.get(job_id)
    if entry is None:
        return {"error": "Demo job expired — start a new one from the wizard."}
    elapsed = time.monotonic() - entry["started_at"]
    total = 8  # cosmetic; matches the number of themes+codes in the payload
    if elapsed < 1:
        return {"id": job_id, "status": "queued",
                "phase": "queued",
                "documents_total": 0, "documents_done": 0,
                "passages_total": 0, "passages_done": 0}
    if elapsed < _DEMO_TIMELINE_SECONDS:
        done = min(total, int((elapsed - 1) / (_DEMO_TIMELINE_SECONDS - 1) * total))
        phase = "extracting_quote_codes" if done < total // 2 else "synthesizing_themes"
        return {"id": job_id, "status": "running",
                "phase": phase,
                "progress_percent": max(2, min(99, int((done / total) * 100))),
                "documents_total": total, "documents_done": done,
                "passages_total": total, "passages_done": done,
                "analysis_units_total": total, "analysis_units_done": done}
    return {"id": job_id, "status": "succeeded",
            "phase": "succeeded",
            "progress_percent": 100,
            "documents_total": total, "documents_done": total,
            "passages_total": total, "passages_done": total,
            "quotes_created": 8, "themes_created": 6, "codes_created": 4,
            "codebook_id": entry["codebook_id"]}


@bp.get("/new/<corpus_id>/auto-demo")
def new_codebook_auto_demo(corpus_id: str):
    """Create a hardcoded sample codebook and play a scripted progress UI.

    Lets you exercise the full mode-2 wizard (progress → review editor → save)
    without an LLM call. The codebook is real and persisted; only the
    generation step is faked."""
    try:
        result = _backend().create_codebook(
            corpus_id=corpus_id,
            name="Sample Codebook (demo)",
            themes=_demo_codebook_nodes(),
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("codebooks.new_codebook_mode_select", corpus_id=corpus_id))

    codebook_id = str(result.get("id") or "")
    job_id = f"{_DEMO_JOB_PREFIX}{codebook_id}"
    _DEMO_JOBS[job_id] = {
        "started_at": time.monotonic(),
        "codebook_id": codebook_id,
    }
    return redirect(
        url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id)
        + f"?new_job={quote_plus(job_id)}&mode=semi"
        + f"&name={quote_plus('Sample Codebook (demo)')}"
    )


@bp.post("/new/jobs/<job_id>/cancel")
def new_codebook_job_cancel(job_id: str):
    try:
        job = _backend().cancel_generation_job(job_id)
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 200
    return jsonify(job)


def _flatten_codebook_relational(codebook: dict) -> list[dict]:
    """Flatten a CodebookDetailSchema tree to relational rows (node_type +
    parent_name). Used for no-change detection against the saved version."""
    rows: list[dict] = []

    def walk(node: dict, parent_name: str | None) -> None:
        raw_type = (node.get("node_type") or "THEME").upper()
        rows.append({
            "node_type": raw_type,
            "name": node.get("name") or node.get("label") or "",
            "description": node.get("description") or "",
            "parent_name": parent_name or "",
        })
        if raw_type == "CODE":
            return
        for child in node.get("children") or []:
            walk(child, node.get("name") or node.get("label") or "")

    for theme in codebook.get("themes") or []:
        walk(theme, None)
    return rows


def _flatten_codebook_for_review(codebook: dict) -> list[dict]:
    """Convert CodebookDetailSchema tree to positional nodes for review.html."""
    nodes: list[dict] = []

    def walk(node: dict, depth: int) -> None:
        raw_type = (node.get("node_type") or "THEME").upper()
        nodes.append({
            "name": node.get("name") or node.get("label") or "",
            "description": node.get("description") or "",
            "indent": depth,
            "is_code": raw_type == "CODE",
        })
        if raw_type == "CODE":
            return
        for child in node.get("children") or []:
            walk(child, depth + 1)

    for theme in codebook.get("themes") or []:
        walk(theme, 0)
    return nodes


def _themes_to_review_nodes(themes: list[dict]) -> list[dict]:
    """Convert relational flat themes (parent_name) to positional nodes for review.html.
    Used when re-rendering the review page after a validation error."""
    name_to_indent: dict[str, int] = {}
    nodes = []
    for t in themes:
        parent = (t.get("parent_name") or "").strip()
        indent = (name_to_indent.get(parent, 0) + 1) if parent else 0
        is_code = (t.get("node_type") or "").upper() == "CODE"
        name_to_indent[t.get("name") or ""] = indent
        nodes.append({
            "name": t.get("name") or "",
            "description": t.get("description") or "",
            "indent": indent,
            "is_code": is_code,
        })
    return nodes


def _parse_review_rows() -> list[dict]:
    """Parse review.html's positional row fields into relational theme dicts.

    The editor no longer exposes THEME/SUBTHEME/CODE; the type is derived:
    CODE if the row is flagged a code, THEME if it has no parent, else SUBTHEME.
    """
    row_names = request.form.getlist("row_names[]")
    row_descriptions = request.form.getlist("row_descriptions[]")
    row_parents = request.form.getlist("row_parents[]")
    row_is_codes = request.form.getlist("row_is_codes[]")

    themes: list[dict] = []
    for name, desc, parent, is_code_flag in zip(
        row_names, row_descriptions, row_parents, row_is_codes
    ):
        name = name.strip()
        desc = desc.strip()
        parent = parent.strip()
        is_code = is_code_flag == "1"
        if is_code:
            node_type = "CODE"
        elif parent:
            node_type = "SUBTHEME"
        else:
            node_type = "THEME"
        themes.append({
            "node_type": node_type,
            "name": name,
            "description": desc,
            "parent_name": parent or None,
        })
    return themes


def _validate_review_themes(codebook_name: str, themes: list[dict]) -> str | None:
    """Validate parsed review rows. Returns an error message, or None if valid."""
    name_set = {t["name"] for t in themes if t["name"]}
    code_names = {t["name"] for t in themes if t["node_type"] == "CODE" and t["name"]}
    if not codebook_name:
        return "Codebook name must not be blank."
    if not themes:
        return "A codebook must contain at least one theme."
    if any(not t["name"] for t in themes):
        return "All rows must have a name."
    for t in themes:
        if t["parent_name"] and t["parent_name"] not in name_set:
            return (
                f"Parent '{t['parent_name']}' for '{t['name']}' "
                "does not exist in this codebook."
            )
        if t["node_type"] == "CODE" and not t["parent_name"]:
            return (
                f"'{t['name']}' is marked as a code but has no parent; "
                "codes must sit under a theme or subtheme."
            )
        if t["parent_name"] in code_names:
            return (
                f"'{t['name']}' is nested under '{t['parent_name']}', which is a code; "
                "codes must be leaf nodes and cannot have children."
            )
    return None


@bp.get("/<codebook_id>/review")
def codebook_review(codebook_id: str) -> str:
    try:
        codebook = _backend().get_codebook(codebook_id)
    except BackendNotFoundError:
        flash("That codebook couldn't be found. It may have been deleted.", "danger")
        return redirect(url_for("codebooks.list_codebooks"))
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return redirect(url_for("codebooks.list_codebooks"))

    corpus_id = str(codebook.get("corpus_id", ""))
    name = codebook.get("name") or "Generated Codebook"
    return render_template(
        "codebooks/review.html",
        corpus_id=corpus_id,
        codebook_id=codebook_id,
        codebook_name=name,
        nodes=_flatten_codebook_for_review(codebook),
        error=None,
    )


@bp.post("/<codebook_id>/review")
def codebook_review_submit(codebook_id: str) -> str:
    """Save the reviewed codebook as a new version."""
    corpus_id = (request.form.get("corpus_id") or "").strip()
    codebook_name = (request.form.get("codebook_name") or "").strip()
    themes = _parse_review_rows()

    def _re_render(error: str):
        return render_template(
            "codebooks/review.html",
            corpus_id=corpus_id,
            codebook_id=codebook_id,
            codebook_name=codebook_name,
            nodes=_themes_to_review_nodes(themes),
            error=error,
        )

    error = _validate_review_themes(codebook_name, themes)
    if error:
        return _re_render(error)

    # No-change detection: compare against the current version.
    try:
        original = _backend().get_codebook(codebook_id)
        original_name = (original.get("name") or "").strip()
        original_themes = _flatten_codebook_relational(original)

        def _norm(rows):
            return [{**r, "parent_name": r.get("parent_name") or ""} for r in rows]

        if codebook_name == original_name and _norm(themes) == _norm(original_themes):
            flash("No changes were made to the codebook.", "info")
            return redirect(url_for("codebooks.success", corpus_id=corpus_id, codebook_id=codebook_id))
    except BackendError:
        pass

    try:
        res = _backend().create_codebook(
            corpus_id=corpus_id, name=codebook_name, themes=themes
        )
        new_id = res["id"]
    except BackendError as exc:
        return _re_render(str(exc))

    return redirect(url_for("codebooks.success", corpus_id=corpus_id, codebook_id=new_id))


@bp.post("/<corpus_id>/<codebook_id>/delete")
def delete_codebook(corpus_id: str, codebook_id: str):
    """Delete a codebook and its themes."""
    force = request.form.get("force_delete") == "1"
    try:
        _backend().delete_codebook(codebook_id, force=force)
        flash("Codebook successfully deleted.", "success")
    except BackendConflictError as exc:
        session[_PENDING_CODEBOOK_DELETE_KEY] = {
            "corpus_id": corpus_id,
            "item_ids": [codebook_id],
            "message": exc.user_message,
        }
    except BackendError as exc:
        flash(exc.user_message, "danger")
    return redirect(url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id))


@bp.post("/<corpus_id>/delete")
def delete_selected_codebooks(corpus_id: str):
    """Delete codebooks selected in the list view."""
    codebook_ids = [item_id for item_id in request.form.getlist("item_ids") if item_id]
    if not codebook_ids:
        flash("Select at least one codebook to delete.", "warning")
        return redirect(url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id))

    force = request.form.get("force_delete") == "1"
    deleted = 0
    try:
        client = _backend()
        for codebook_id in codebook_ids:
            client.delete_codebook(codebook_id, force=force)
            deleted += 1
        flash(f"Deleted {deleted} codebook{'s' if deleted != 1 else ''}.", "success")
    except BackendConflictError as exc:
        if deleted:
            flash(f"Deleted {deleted} codebook{'s' if deleted != 1 else ''} before an error occurred.", "warning")
        if not force:
            session[_PENDING_CODEBOOK_DELETE_KEY] = {
                "corpus_id": corpus_id,
                "item_ids": codebook_ids[deleted:],
                "message": exc.user_message,
            }
        else:
            flash(exc.user_message, "danger")
    except BackendError as exc:
        if deleted:
            flash(f"Deleted {deleted} codebook{'s' if deleted != 1 else ''} before an error occurred.", "warning")
        flash(exc.user_message, "danger")

    return redirect(url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id))
