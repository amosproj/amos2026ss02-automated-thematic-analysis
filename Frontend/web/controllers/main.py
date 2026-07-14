import json
from pathlib import Path

from flask import Blueprint, flash, redirect, render_template, request, url_for

from web.services.backend_client import BackendError, get_backend_client as _backend

bp = Blueprint("main", __name__)
_LEGAL_NOTICES_PATH = Path(__file__).resolve().parents[1] / "static" / "legal_notices.json"

# Shown when the backend is unreachable so the Home page still renders a sane,
# read-only provider card instead of erroring out.
_FALLBACK_PROVIDER_STATE: dict = {
    "active": "FAU",
    "default": "FAU",
    "available": [
        {"id": "FAU", "label": "FAU NHR",
         "description": "The university's NHR@FAU gateway (default).",
         "has_api_key": True},
        {"id": "ACADEMIC", "label": "Academic Cloud",
         "description": "The GWDG Academic Cloud chat-ai endpoint.",
         "has_api_key": True},
    ],
}


@bp.get("/")
def index() -> str:
    provider_state = _FALLBACK_PROVIDER_STATE
    provider_available = False
    try:
        provider_state = _backend().get_llm_provider()
        provider_available = True
    except BackendError as exc:
        # Non-fatal: render the Home page with a disabled provider card.
        flash(
            f"Couldn't load the LLM provider setting: {exc.user_message}",
            "warning",
        )
    return render_template(
        "index.html",
        provider_state=provider_state,
        provider_available=provider_available,
    )


@bp.post("/settings/llm-provider")
def set_llm_provider():
    provider = (request.form.get("provider") or "").strip()
    if not provider:
        flash("Please choose an LLM provider before saving.", "danger")
        return redirect(url_for("main.index"))

    try:
        state = _backend().set_llm_provider(provider)
        label = next(
            (opt["label"] for opt in state.get("available", []) if opt["id"] == state["active"]),
            state["active"],
        )
        flash(f"LLM provider set to {label}.", "success")
    except BackendError as exc:
        flash(exc.user_message, "danger")
    return redirect(url_for("main.index"))


@bp.get("/help")
def help_page() -> str:
    return render_template("help.html")


@bp.get("/legal-notices")
def legal_notices() -> str:
    with _LEGAL_NOTICES_PATH.open(encoding="utf-8") as notices_file:
        notices = json.load(notices_file)
    return render_template("legal_notices.html", notices=notices)


@bp.get("/health")
def health() -> dict:
    """Liveness probe for Docker HEALTHCHECK; does not check the backend."""
    return {"status": "ok"}

@bp.get("/slide")
def presentation_slide1() -> str:
    """Slide 1: What is Thematic Analysis?"""
    return render_template("slide1.html")


@bp.get("/slide1")
def presentation_slide2() -> str:
    """Slide 2: Our Solution."""
    return render_template("slide.html")


@bp.get("/slide2")
def presentation_slide3() -> str:
    """Codebook Generation Algorithm"""
    return render_template("slide2.html")
