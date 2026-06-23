"""Central registry of the LLM providers the app can route AI tasks to.

The app supports two pre-configured providers whose credentials live in
``Settings`` (env / .env):

* ``FAU``      → NHR@FAU gateway   (``LLM_*_FAU`` settings)
* ``ACADEMIC`` → Academic Cloud    (``LLM_*`` settings)

This module is the single source of truth for the provider id, its
human-readable label, and which ``Settings`` attributes hold its credentials.
Both the LLM client and the settings API/UI read from here so labels and ids
never drift between layers.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings


@dataclass(frozen=True)
class ProviderSpec:
    """Static description of one selectable LLM provider."""

    id: str  # canonical, stored/transported value (always upper-case)
    label: str  # human-readable name shown in the UI
    description: str  # short, non-technical explanation for tooltips
    api_key_attr: str  # Settings attribute holding the API key
    base_url_attr: str  # Settings attribute holding the base URL
    model_attr: str  # Settings attribute holding the default model name


# Order matters: the first entry is treated as the built-in default when no
# stored selection and no env override resolve to a known provider.
PROVIDERS: tuple[ProviderSpec, ...] = (
    ProviderSpec(
        id="FAU",
        label="FAU NHR",
        description=(
            "The university's NHR@FAU gateway. This is the default and uses the "
            "setup the team has relied on so far."
        ),
        api_key_attr="LLM_API_KEY_FAU",
        base_url_attr="LLM_BASE_URL_FAU",
        model_attr="LLM_MODEL_FAU",
    ),
    ProviderSpec(
        id="ACADEMIC",
        label="Academic Cloud",
        description=(
            "The GWDG Academic Cloud chat-ai endpoint. Use this to route AI "
            "tasks through the Academic Cloud models instead of FAU NHR."
        ),
        api_key_attr="LLM_API_KEY",
        base_url_attr="LLM_BASE_URL",
        model_attr="LLM_MODEL",
    ),
)

_PROVIDERS_BY_ID: dict[str, ProviderSpec] = {spec.id: spec for spec in PROVIDERS}

DEFAULT_PROVIDER_ID: str = PROVIDERS[0].id


def available_providers() -> tuple[ProviderSpec, ...]:
    """Return every selectable provider, in display order."""
    return PROVIDERS


def is_known_provider(value: str | None) -> bool:
    """True when ``value`` (case-insensitive) names a registered provider."""
    return value is not None and value.strip().upper() in _PROVIDERS_BY_ID


def normalize(value: str | None) -> str | None:
    """Return the canonical provider id for ``value`` or ``None`` if unknown."""
    if not value:
        return None
    candidate = value.strip().upper()
    return candidate if candidate in _PROVIDERS_BY_ID else None


def get_provider(value: str | None) -> ProviderSpec | None:
    """Return the :class:`ProviderSpec` for ``value`` or ``None`` if unknown."""
    canonical = normalize(value)
    return _PROVIDERS_BY_ID.get(canonical) if canonical else None


def resolve_default(settings: Settings) -> str:
    """Resolve the env-configured default provider, falling back to FAU.

    Mirrors the historic ``SELECTED_API`` semantics: any unrecognised value
    falls back to the built-in default so the app always has a valid provider.
    """
    return normalize(settings.SELECTED_API) or DEFAULT_PROVIDER_ID


def has_api_key(settings: Settings, provider_id: str) -> bool:
    """True when the credentials for ``provider_id`` include a non-empty key."""
    spec = get_provider(provider_id)
    if spec is None:
        return False
    return bool(getattr(settings, spec.api_key_attr, None))
