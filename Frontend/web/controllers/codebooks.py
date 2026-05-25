from flask import Blueprint, flash, redirect, render_template, request, url_for, current_app

from web.services.backend_client import (
    BackendError,
    BackendNotFoundError,
    get_backend_client as _backend,
)
from web.services.corpus_context import resolve_active_corpus, set_active_corpus_id

bp = Blueprint("codebooks", __name__)


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
    )

@bp.get("/upload")
def upload_form() -> str:
    """Render the upload form (choose CSV or manual)."""
    return render_template("codebooks/upload.html", error=None)

@bp.post("/upload")
def upload_submit() -> str:
    """Handle either CSV file upload or redirect to manual entry."""
    action = request.form.get("action", "upload")

    if action == "manual":
        return redirect(url_for("codebooks.manual_form"))

    # CSV file upload path
    file = request.files.get("file")
    if not file or not file.filename:
        return render_template(
            "codebooks/upload.html",
            error="Please select a CSV file to upload or choose manual entry.",
        )

    if not file.filename.lower().endswith(".csv"):
        return render_template(
            "codebooks/upload.html",
            error="Only CSV files (.csv extension) are supported.",
        )

    try:
        client = _backend()
        parsed_themes = client.parse_csv_preview(file)
        # Derive a readable default name from the file name
        default_name = file.filename.rsplit(".", 1)[0].replace("_", " ").title()
        return render_template(
            "codebooks/preview.html",
            codebook_name=default_name,
            themes=parsed_themes,
            error=None,
        )
    except BackendError as exc:
        return render_template("codebooks/upload.html", error=str(exc))

@bp.get("/manual")
def manual_form() -> str:
    """Render the preview editor pre-filled with one blank theme row."""
    empty_themes = [{"name": "", "description": ""}]
    return render_template(
        "codebooks/preview.html",
        codebook_name="New Codebook",
        themes=empty_themes,
        error=None,
    )

@bp.post("/confirm")
def confirm_submit() -> str:
    """Validate, customise, and confirm a codebook and its themes."""
    codebook_name = (request.form.get("codebook_name") or "").strip()
    theme_names = request.form.getlist("theme_names[]")
    theme_descriptions = request.form.getlist("theme_descriptions[]")

    # Assemble themes back into expected structure
    themes = []
    for name, desc in zip(theme_names, theme_descriptions):
        themes.append({"name": name.strip(), "description": desc.strip()})

    # Frontend validation
    error = None
    if not codebook_name:
        error = "Codebook Name must not be blank."
    elif not themes:
        error = "A codebook must contain at least one theme."
    elif any(not t["name"] for t in themes):
        error = "All themes must have a name."

    if error:
        return render_template(
            "codebooks/preview.html",
            codebook_name=codebook_name,
            themes=themes,
            error=error,
        )

    try:
        client = _backend()
        project_id = current_app.config["DEFAULT_PROJECT_ID"]
        res = client.create_codebook(project_id, codebook_name, themes)
        codebook_id = res["id"]
        return redirect(url_for("codebooks.success", codebook_id=codebook_id))
    except BackendError as exc:
        return render_template(
            "codebooks/preview.html",
            codebook_name=codebook_name,
            themes=themes,
            error=str(exc),
        )

@bp.get("/success")
def success() -> str:
    """Show details of the successfully saved codebook."""
    codebook_id = request.args.get("codebook_id")
    if not codebook_id:
        return redirect(url_for("codebooks.upload_form"))

    try:
        client = _backend()
        codebook = client.get_codebook(codebook_id)
        return render_template("codebooks/success.html", codebook=codebook, error=None)
    except BackendError as exc:
        return render_template("codebooks/success.html", codebook=None, error=str(exc))
