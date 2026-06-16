import csv
import io
import time
import zipfile
from urllib.parse import quote_plus

from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from web.services.backend_client import (
    BackendClient,
    BackendError,
    BackendNotFoundError,
    get_backend_client as _backend,
)
from web.services.corpus_context import resolve_active_corpus, set_active_corpus_id

bp = Blueprint("codebooks", __name__)

CODING_MODES = ("auto", "semi", "manual")


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
            error=True,
        )
    return render_template(
        "codebooks/list.html",
        codebooks=codebooks,
        corpus_id=active_corpus_id,
        corpus_options=corpus_options,
        active_corpus_name=corpus_name,
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
            )
        )
    return redirect(url_for("codebooks.list_codebooks"))


@bp.get("/<corpus_id>/<codebook_id>/themes")
def codebook_themes_for_corpus(corpus_id: str, codebook_id: str) -> str:
    set_active_corpus_id(corpus_id)
    name = request.args.get("name", "")
    version = request.args.get("version", "")
    active_codebook_id = codebook_id
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

        frequencies = client.get_theme_frequencies(active_codebook_id)
        tree = client.get_theme_tree(active_codebook_id)
        codebook = client.get_codebook(active_codebook_id)
        codes = codebook.get("codes", [])
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
    )

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
            "codebooks/preview.html",
            corpus_id=corpus_id,
            codebook_name=default_name,
            themes=parsed_themes,
            error=None,
        )
    except BackendError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("ingestion.upload_form", corpus_id=corpus_id))

@bp.get("/<corpus_id>/manual")
def manual_form(corpus_id: str) -> str:
    """Render the preview editor pre-filled with one blank node row."""
    empty_nodes = [{"node_type": "THEME", "name": "", "description": "", "parent_name": ""}]
    return render_template(
        "codebooks/preview.html",
        corpus_id=corpus_id,
        codebook_name="New Codebook",
        themes=empty_nodes,
        error=None,
    )

@bp.post("/<corpus_id>/confirm")
def confirm_submit(corpus_id: str) -> str:
    """Validate, customise, and confirm a codebook and its themes."""
    codebook_name = (request.form.get("codebook_name") or "").strip()
    node_types = request.form.getlist("node_types[]")
    theme_names = request.form.getlist("theme_names[]")
    theme_descriptions = request.form.getlist("theme_descriptions[]")
    parent_names = request.form.getlist("parent_names[]")

    # Assemble themes back into expected structure
    themes = []
    for nt, name, desc, parent in zip(node_types, theme_names, theme_descriptions, parent_names):
        themes.append({
            "node_type": nt,
            "name": name.strip(),
            "description": desc.strip(),
            "parent_name": parent.strip() if parent.strip() else None
        })

    # Frontend validation
    error = None
    theme_names_set = {t["name"] for t in themes if t["name"]}

    if not codebook_name:
        error = "Codebook Name must not be blank."
    elif not themes:
        error = "A codebook must contain at least one theme."
    elif any(not t["name"] for t in themes):
        error = "All themes must have a name."
    else:
        for t in themes:
            if t["node_type"] == "SUBTHEME" and not t["parent_name"]:
                error = f"Node '{t['name']}' of type {t['node_type']} must have a Parent Name."
                break
            if t["node_type"] == "THEME" and t["parent_name"]:
                error = f"Node '{t['name']}' of type {t['node_type']} must not have a Parent Name."
                break
            if t["parent_name"] and t["parent_name"] not in theme_names_set:
                error = f"Parent '{t['parent_name']}' for theme '{t['name']}' does not exist in this codebook."
                break

    if error:
        return render_template(
            "codebooks/preview.html",
            corpus_id=corpus_id,
            codebook_name=codebook_name,
            themes=themes,
            error=error,
        )

    try:
        client = _backend()
        res = client.create_codebook(corpus_id, codebook_name, themes)
        codebook_id = res["id"]
        return redirect(url_for("codebooks.success", corpus_id=corpus_id, codebook_id=codebook_id))
    except BackendError as exc:
        return render_template(
            "codebooks/preview.html",
            corpus_id=corpus_id,
            codebook_name=codebook_name,
            themes=themes,
            error=str(exc),
        )

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


@bp.get("/new/<corpus_id>/auto")
def new_codebook_auto_form(corpus_id: str) -> str:
    mode = _resolve_mode(request.args.get("mode", ""))
    return render_template(
        "codebooks/new/auto_form.html",
        corpus_id=corpus_id,
        mode=mode,
        codebook_name=request.args.get("name", ""),
    )


@bp.post("/new/<corpus_id>/auto")
def new_codebook_auto_submit(corpus_id: str):
    mode = _resolve_mode(request.form.get("mode", ""))
    name = (request.form.get("codebook_name") or "").strip()
    if not name:
        flash("Please give your codebook a name.", "danger")
        return render_template(
            "codebooks/new/auto_form.html",
            corpus_id=corpus_id,
            mode=mode,
            codebook_name="",
        )

    try:
        job = _backend().create_generation_job(
            codebook_name=name,
            corpus_id=corpus_id,
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return render_template(
            "codebooks/new/auto_form.html",
            corpus_id=corpus_id,
            mode=mode,
            codebook_name=name,
        )

    # Hand off to the codebook list with new-job query params; job_tracker.js
    # picks them up, persists the job in localStorage, and starts polling in
    # the background so the user can navigate freely until generation finishes.
    return redirect(
        url_for("codebooks.list_codebooks_for_corpus", corpus_id=corpus_id)
        + f"?new_job={quote_plus(str(job['id']))}"
        + f"&mode={mode}&name={quote_plus(name)}"
    )


@bp.get("/new/jobs/<job_id>")
def new_codebook_job_progress(job_id: str) -> str:
    mode = _resolve_mode(request.args.get("mode", ""))
    return render_template(
        "codebooks/new/progress.html",
        job_id=job_id,
        mode=mode,
    )


@bp.get("/new/jobs/<job_id>.json")
def new_codebook_job_status(job_id: str):
    # Errors come back as 200 with `{error: "..."}` so the poller doesn't need
    # to distinguish HTTP failure modes.
    if job_id.startswith(_DEMO_JOB_PREFIX):
        return jsonify(_demo_job_state(job_id))
    try:
        job = _backend().get_generation_job(job_id)
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
                "passages_total": 0, "passages_done": 0}
    if elapsed < _DEMO_TIMELINE_SECONDS:
        done = min(total, int((elapsed - 1) / (_DEMO_TIMELINE_SECONDS - 1) * total))
        return {"id": job_id, "status": "running",
                "passages_total": total, "passages_done": done}
    return {"id": job_id, "status": "succeeded",
            "passages_total": total, "passages_done": total,
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


def _flatten_codebook_for_editor(codebook: dict) -> list[dict]:
    """Walk CodebookDetailSchema into a flat ordered list of editor rows.

    Each row is `{name, description, indent, is_code}`. Hierarchy is captured
    as depth: indent 0 = root theme, deeper = subtheme. Code nodes (leaves
    attached to themes via ThemeCodeRelationship) carry is_code=True so the
    editor can render and round-trip them with the right type badge."""
    rows: list[dict] = []

    def walk(node: dict, depth: int) -> None:
        is_code = (node.get("node_type") or "").upper() == "CODE"
        rows.append({
            "name": node.get("name") or node.get("label") or "",
            "description": node.get("description") or "",
            "indent": depth,
            "is_code": is_code,
        })
        # Codes are leaves; only walk children of non-code nodes.
        if is_code:
            return
        for child in node.get("children") or []:
            walk(child, depth + 1)

    for theme in codebook.get("themes") or []:
        walk(theme, 0)
    return rows


def _render_review(codebook_id: str, codebook_name: str, corpus_id: str,
                   nodes: list[dict], error: str | None = None) -> str:
    return render_template(
        "codebooks/new/review.html",
        codebook_id=codebook_id,
        codebook_name=codebook_name,
        corpus_id=corpus_id,
        nodes=nodes,
        error=error,
    )


# Branch 9's NodeInput requires description min_length=1; the LLM occasionally
# produces nodes with null description and the editor allows blank rows. Fill
# with a placeholder rather than reject so the save succeeds end-to-end.
_DESCRIPTION_PLACEHOLDER = "(no description)"


@bp.get("/<codebook_id>/review")
def codebook_review(codebook_id: str) -> str:
    try:
        codebook = _backend().get_codebook(codebook_id)
    except BackendNotFoundError:
        flash("That codebook couldn't be found. It may have been deleted.", "danger")
        return _render_review(codebook_id, "", "", [], error="Codebook not found.")
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return _render_review(codebook_id, "", "", [], error=exc.user_message)

    corpus_id = str(codebook.get("corpus_id", ""))
    name = codebook.get("name") or "Generated Codebook"
    return _render_review(
        codebook_id, name, corpus_id, _flatten_codebook_for_editor(codebook),
    )


@bp.post("/<codebook_id>/review")
def codebook_review_submit(codebook_id: str):
    codebook_name = (request.form.get("codebook_name") or "").strip()
    corpus_id = (request.form.get("corpus_id") or "").strip()
    names = request.form.getlist("row_names[]")
    descs = request.form.getlist("row_descriptions[]")
    parents = request.form.getlist("row_parents[]")
    is_codes = request.form.getlist("row_is_codes[]")

    # Mirror the preview/confirm_submit pattern: assemble all rows first,
    # then validate. Don't silently skip blank-named rows — preview errors
    # on them so they get the same treatment here.
    def at(seq: list[str], i: int) -> str:
        return seq[i] if i < len(seq) else ""

    rows: list[dict] = []
    for i, name in enumerate(names):
        rows.append({
            "name": name.strip(),
            "description": at(descs, i).strip(),
            "parent_name": at(parents, i).strip() or None,
            "is_code": at(is_codes, i) == "1",
        })

    # Editor-shaped rows for the redisplay path so a re-render preserves
    # what the user typed, including the Code toggle.
    redisplay_rows = [
        {
            "name": r["name"],
            "description": r["description"],
            "indent": 0 if not r["parent_name"] else 1,
            "is_code": r["is_code"],
        }
        for r in rows
    ]

    error: str | None = None
    if not codebook_name:
        error = "Codebook Name must not be blank."
    elif not corpus_id:
        error = "Missing corpus context — refresh the page and try again."
    elif not rows:
        error = "A codebook must contain at least one theme."
    elif any(not r["name"] for r in rows):
        error = "All themes must have a name."
    else:
        # Parent lookups span themes and subthemes only — a code can't be a
        # parent.
        valid_parents = {r["name"] for r in rows if not r["is_code"]}
        for r in rows:
            if r["is_code"] and not r["parent_name"]:
                error = (
                    f"Code '{r['name']}' must sit under a theme or subtheme. "
                    "Either indent it under one, or untoggle 'Code'."
                )
                break
            if r["parent_name"] and r["parent_name"] not in valid_parents:
                error = (
                    f"Parent '{r['parent_name']}' for '{r['name']}' "
                    "does not exist in this codebook."
                )
                break

    if error:
        return _render_review(codebook_id, codebook_name, corpus_id, redisplay_rows, error=error)

    # Build the backend payload: descriptions default to a placeholder so the
    # backend's NodeInput.description (min_length=1) doesn't reject the row.
    payload_nodes: list[dict] = []
    for r in rows:
        entry: dict = {
            "name": r["name"],
            "description": r["description"] or _DESCRIPTION_PLACEHOLDER,
            "parent_name": r["parent_name"],
        }
        if r["is_code"]:
            entry["node_type"] = "CODE"
        payload_nodes.append(entry)

    try:
        _backend().create_codebook(
            corpus_id=corpus_id, name=codebook_name, themes=payload_nodes,
        )
    except BackendError as exc:
        return _render_review(codebook_id, codebook_name, corpus_id, redisplay_rows, error=exc.user_message)

    flash(f"Saved '{codebook_name}' as a new codebook version.", "success")
    return redirect(url_for("codebooks.list_codebooks"))
