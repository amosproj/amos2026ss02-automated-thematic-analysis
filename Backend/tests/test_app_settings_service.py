"""Tests for the app-settings service (active LLM provider get/set)."""
from __future__ import annotations

import pytest

from app.config import Settings
from app.exceptions import UnprocessableError
from app.services.app_settings import get_active_provider, set_active_provider

pytestmark = pytest.mark.asyncio


def _settings(**overrides) -> Settings:
    base = dict(
        DATABASE_URL="sqlite+aiosqlite:///:memory:",
        LLM_API_KEY_FAU="fau-key",
        LLM_API_KEY="academic-key",
    )
    base.update(overrides)
    return Settings(**base)


async def test_get_active_provider_defaults_to_env(db_session) -> None:
    cfg = _settings(SELECTED_API="ACADEMIC")
    assert await get_active_provider(db_session, settings=cfg) == "ACADEMIC"


async def test_get_active_provider_default_fau_when_unset(db_session) -> None:
    cfg = _settings(SELECTED_API="FAU")
    assert await get_active_provider(db_session, settings=cfg) == "FAU"


async def test_set_then_get_round_trip(db_session) -> None:
    cfg = _settings(SELECTED_API="FAU")
    result = await set_active_provider(db_session, "academic", settings=cfg)
    assert result == "ACADEMIC"
    # Stored value overrides the env default on subsequent reads.
    assert await get_active_provider(db_session, settings=cfg) == "ACADEMIC"


async def test_set_updates_existing_row(db_session) -> None:
    cfg = _settings()
    await set_active_provider(db_session, "ACADEMIC", settings=cfg)
    await set_active_provider(db_session, "FAU", settings=cfg)
    assert await get_active_provider(db_session, settings=cfg) == "FAU"


async def test_set_unknown_provider_raises(db_session) -> None:
    cfg = _settings()
    with pytest.raises(UnprocessableError, match="Unknown LLM provider"):
        await set_active_provider(db_session, "litellm", settings=cfg)


async def test_set_provider_without_key_raises(db_session) -> None:
    cfg = _settings(LLM_API_KEY=None)  # Academic Cloud has no key
    with pytest.raises(UnprocessableError, match="no API key configured"):
        await set_active_provider(db_session, "ACADEMIC", settings=cfg)


async def test_get_falls_back_when_stored_value_unknown(db_session) -> None:
    # Simulate a stale stored value by writing directly, then reading.
    from app.models.app_settings import ACTIVE_LLM_PROVIDER_KEY, AppSetting

    db_session.add(AppSetting(key=ACTIVE_LLM_PROVIDER_KEY, value="LITELLM"))
    await db_session.commit()
    cfg = _settings(SELECTED_API="ACADEMIC")
    assert await get_active_provider(db_session, settings=cfg) == "ACADEMIC"
