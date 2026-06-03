from __future__ import annotations

from flask import current_app, session

from web.services.backend_client import BackendClient, BackendNotFoundError

ACTIVE_CORPUS_SESSION_KEY = "active_corpus_id"


def list_workspace_corpora(client: BackendClient) -> list[dict]:
    """Return all corpora for the configured workspace project.

    Ensures at least one corpus exists by creating the default corpus on demand.
    """
    cfg = current_app.config
    corpus_id = cfg["DEFAULT_CORPUS_ID"]
    corpora = client.list_corpora()
    if corpora:
        return corpora

    ensured_id = client.ensure_corpus(
        corpus_id=corpus_id,
        name=cfg["DEFAULT_CORPUS_NAME"],
    )
    corpora = client.list_corpora()
    if corpora:
        return corpora
    return [{"id": ensured_id, "name": cfg["DEFAULT_CORPUS_NAME"]}]


def set_active_corpus_id(corpus_id: str) -> None:
    session[ACTIVE_CORPUS_SESSION_KEY] = corpus_id


def resolve_active_corpus(
    client: BackendClient,
    *,
    requested_corpus_id: str | None = None,
    strict_requested: bool = False,
) -> tuple[str, list[dict], dict]:
    """Resolve the active corpus id and persist it in session.

    Priority:
    1. requested_corpus_id from route/query
    2. session-stored active corpus id
    3. first available corpus in the project

    When strict_requested=True and requested_corpus_id is provided but missing,
    raise BackendNotFoundError instead of silently falling back.
    """
    corpora = list_workspace_corpora(client)
    corpora_by_id = {str(c["id"]): c for c in corpora}

    if (
        strict_requested
        and requested_corpus_id
        and requested_corpus_id not in corpora_by_id
    ):
        raise BackendNotFoundError(
            user_message=(
                "The selected corpus couldn't be found. "
                "Please choose another corpus."
            )
        )

    selected_id: str | None = None
    for candidate in (requested_corpus_id, session.get(ACTIVE_CORPUS_SESSION_KEY)):
        if candidate and candidate in corpora_by_id:
            selected_id = candidate
            break

    if selected_id is None:
        selected_id = str(corpora[0]["id"])

    session[ACTIVE_CORPUS_SESSION_KEY] = selected_id
    return selected_id, corpora, corpora_by_id[selected_id]
