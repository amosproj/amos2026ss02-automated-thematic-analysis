import csv
import io
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

    return redirect(url_for("codebooks.upload_form", corpus_id=active_corpus_id))


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
        themes = codebook.get("themes", [])
        codes = codebook.get("codes", [])

        flat_rows = []
        exported_ids = set()

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

        for t in themes:
            traverse(t, "")

        for c in codes:
            if c.get("id") not in exported_ids:
                flat_rows.append({
                    "Node Type": "CODE",
                    "Name": c.get("name", ""),
                    "Description": c.get("description", ""),
                    "Parent Name": "",
                })

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["Node Type", "Name", "Description", "Parent Name"])
        writer.writeheader()
        writer.writerows(flat_rows)

        csv_data = output.getvalue()
        filename = f"{codebook.get('name', 'codebook').replace(' ', '_')}_v{codebook.get('version', 1)}.csv"

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


@bp.get("/<corpus_id>/upload")
def upload_form(corpus_id: str) -> str:
    """Render the upload form (choose CSV or manual)."""
    set_active_corpus_id(corpus_id)
    try:
        active_corpus_id, corpus_options, _ = resolve_active_corpus(
            _backend(),
            requested_corpus_id=corpus_id,
        )
    except BackendError as exc:
        flash(exc.user_message, "danger")
        active_corpus_id = corpus_id
        corpus_options = []

    return render_template("codebooks/upload.html", corpus_id=active_corpus_id, corpus_options=corpus_options, error=None)

@bp.post("/<corpus_id>/upload")
def upload_submit(corpus_id: str) -> str:
    """Handle either CSV file upload or redirect to manual entry."""
    action = request.form.get("action", "upload")

    if action == "manual":
        return redirect(url_for("codebooks.manual_form", corpus_id=corpus_id))

    # CSV file upload path
    file = request.files.get("file")
    if not file or not file.filename:
        return render_template(
            "codebooks/upload.html",
            corpus_id=corpus_id,
            error="Please select a CSV file to upload or choose manual entry.",
        )

    if not file.filename.lower().endswith(".csv"):
        return render_template(
            "codebooks/upload.html",
            corpus_id=corpus_id,
            error="Only CSV files (.csv extension) are supported.",
        )

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
        return render_template("codebooks/upload.html", corpus_id=corpus_id, error=str(exc))

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
        return redirect(url_for("codebooks.upload_form", corpus_id=corpus_id))

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

    # mode == "manual": branch 9's upload form is corpus-scoped.
    return redirect(url_for("codebooks.upload_form", corpus_id=corpus_id))


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

    return redirect(
        url_for("codebooks.new_codebook_job_progress", job_id=job["id"], mode=mode)
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
    try:
        job = _backend().get_generation_job(job_id)
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 200
    return jsonify(job)


@bp.post("/new/jobs/<job_id>/cancel")
def new_codebook_job_cancel(job_id: str):
    try:
        job = _backend().cancel_generation_job(job_id)
    except BackendError as exc:
        return jsonify({"error": exc.user_message}), 200
    return jsonify(job)


def _flatten_theme_tree(tree: list[dict]) -> list[dict]:
    """Flatten get_theme_tree into editor rows: name, description, parent_name.

    Branch 9's theme tree includes CODE nodes alongside THEMEs. The review
    editor surfaces codes separately via the codebook detail's `codes[]`
    field, so we skip CODE-typed nodes here to avoid duplicating them."""
    rows: list[dict] = []

    def walk(node: dict, parent_label: str | None) -> None:
        theme = node.get("theme") or {}
        if theme.get("node_type") == "CODE":
            return
        label = theme.get("label", "")
        rows.append({
            "name": label,
            "description": theme.get("description") or "",
            "parent_name": parent_label or "",
        })
        for child in node.get("children") or []:
            walk(child, label)

    for root in tree:
        walk(root, None)
    return rows


def _render_review(codebook_id: str, codebook_name: str, corpus_id: str,
                   themes: list[dict], error: str | None = None) -> str:
    return render_template(
        "codebooks/new/review.html",
        codebook_id=codebook_id,
        codebook_name=codebook_name,
        corpus_id=corpus_id,
        themes=themes,
        error=error,
    )


# Branch 9's NodeInput requires description min_length=1; the LLM occasionally
# produces themes with null description and the editor allows blank rows. Fill
# with a placeholder rather than reject so the save succeeds end-to-end.
_DESCRIPTION_PLACEHOLDER = "(no description)"


@bp.get("/<codebook_id>/review")
def codebook_review(codebook_id: str) -> str:
    try:
        client = _backend()
        tree = client.get_theme_tree(codebook_id)
        codebook = client.get_codebook(codebook_id)
    except BackendNotFoundError:
        flash("That codebook couldn't be found. It may have been deleted.", "danger")
        return _render_review(codebook_id, "", "", [], error="Codebook not found.")
    except BackendError as exc:
        flash(exc.user_message, "danger")
        return _render_review(codebook_id, "", "", [], error=exc.user_message)

    corpus_id = str(codebook.get("corpus_id", ""))
    name = codebook.get("name") or "Generated Codebook"
    return _render_review(codebook_id, name, corpus_id, _flatten_theme_tree(tree))


@bp.post("/<codebook_id>/review")
def codebook_review_submit(codebook_id: str):
    codebook_name = (request.form.get("codebook_name") or "").strip()
    corpus_id = (request.form.get("corpus_id") or "").strip()
    names = request.form.getlist("theme_names[]")
    descriptions = request.form.getlist("theme_descriptions[]")
    parents = request.form.getlist("parent_names[]")

    themes: list[dict] = []
    for name, desc, parent in zip(names, descriptions, parents):
        cleaned_name = name.strip()
        if not cleaned_name:
            continue
        themes.append({
            "name": cleaned_name,
            "description": desc.strip() or _DESCRIPTION_PLACEHOLDER,
            "parent_name": parent.strip() or None,
        })

    name_set = {t["name"] for t in themes}
    error: str | None = None
    if not codebook_name:
        error = "Codebook name must not be blank."
    elif not corpus_id:
        error = "Missing corpus context — refresh the page and try again."
    elif not themes:
        error = "A codebook must contain at least one theme."
    else:
        for t in themes:
            if t["parent_name"] and t["parent_name"] not in name_set:
                error = f"Parent '{t['parent_name']}' for theme '{t['name']}' does not exist."
                break

    if error:
        return _render_review(codebook_id, codebook_name, corpus_id, themes, error=error)

    try:
        _backend().create_codebook(
            corpus_id=corpus_id,
            name=codebook_name,
            themes=themes,
        )
    except BackendError as exc:
        return _render_review(codebook_id, codebook_name, corpus_id, themes, error=exc.user_message)

    flash(f"Saved '{codebook_name}' as a new codebook version.", "success")
    return redirect(url_for("codebooks.list_codebooks"))
