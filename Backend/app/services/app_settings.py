"""Read/write access to mutable, app-wide settings.

Currently this covers the active LLM provider: a single global value that
every AI task reads at run start. Resolution order when reading:

    stored DB value (if a known provider) → env ``SELECTED_API`` default → FAU

Writes validate against the provider registry so an unknown id can never be
persisted.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.exceptions import UnprocessableError
from app.llm import providers
from app.models.app_settings import ACTIVE_LLM_PROVIDER_KEY, AppSetting


async def get_active_provider(
    session: AsyncSession,
    *,
    settings: Settings | None = None,
) -> str:
    """Return the canonical id of the active LLM provider.

    Falls back to the env default when nothing is stored yet, or when a
    previously stored value is no longer a registered provider.
    """
    cfg = settings or get_settings()
    stored = (
        await session.execute(
            select(AppSetting.value).where(AppSetting.key == ACTIVE_LLM_PROVIDER_KEY)
        )
    ).scalar_one_or_none()
    return providers.normalize(stored) or providers.resolve_default(cfg)


async def set_active_provider(
    session: AsyncSession,
    provider: str,
    *,
    settings: Settings | None = None,
) -> str:
    """Persist the active LLM provider and return its canonical id.

    Raises :class:`UnprocessableError` when the provider is unknown or has no
    API key configured — surfaced to the user so they don't select a provider
    that would fail at task time.
    """
    cfg = settings or get_settings()
    canonical = providers.normalize(provider)
    if canonical is None:
        known = ", ".join(spec.id for spec in providers.available_providers())
        raise UnprocessableError(
            f"Unknown LLM provider '{provider}'. Choose one of: {known}."
        )
    if not providers.has_api_key(cfg, canonical):
        spec = providers.get_provider(canonical)
        label = spec.label if spec else canonical
        raise UnprocessableError(
            f"{label} has no API key configured on the server. "
            "Add the matching key to the backend configuration before selecting it."
        )

    existing = await session.get(AppSetting, ACTIVE_LLM_PROVIDER_KEY)
    if existing is None:
        session.add(AppSetting(key=ACTIVE_LLM_PROVIDER_KEY, value=canonical))
    else:
        existing.value = canonical
    await session.commit()
    return canonical
