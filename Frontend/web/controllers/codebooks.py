from flask import Blueprint, flash, redirect, render_template, request, url_for

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
