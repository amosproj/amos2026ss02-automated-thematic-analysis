"""Integration tests for the LLM provider settings API.

Uses a dedicated app client that overrides both the DB session and Settings so
the test controls which providers have API keys configured, independent of the
ambient .env.
"""
from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings

pytestmark = pytest.mark.asyncio

PREFIX = "/api/v1/settings/llm-provider"


def _settings(**overrides) -> Settings:
    base = dict(
        DATABASE_URL="sqlite+aiosqlite:///:memory:",
        LLM_API_KEY_FAU="fau-key",
        LLM_API_KEY="academic-key",
        SELECTED_API="FAU",
    )
    base.update(overrides)
    return Settings(**base)


@pytest_asyncio.fixture
async def provider_client(db_engine) -> AsyncGenerator[tuple[AsyncClient, Settings], None]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    test_settings = _settings()

    async def override_session() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    with (
        patch("app.database.check_db_connection", new=AsyncMock(return_value=True)),
        patch("app.database.init_db", new=AsyncMock()),
        patch("app.database.dispose_engine", new=AsyncMock()),
    ):
        from app.config import get_settings
        from app.database import get_session
        from app.main import create_app

        app = create_app()
        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[get_settings] = lambda: test_settings

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, test_settings


async def test_get_returns_default_and_options(provider_client) -> None:
    client, _ = provider_client
    resp = await client.get(PREFIX)
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    data = body["data"]
    assert data["active"] == "FAU"
    assert data["default"] == "FAU"
    ids = [opt["id"] for opt in data["available"]]
    assert ids == ["FAU", "ACADEMIC"]
    assert all("has_api_key" in opt and "label" in opt for opt in data["available"])


async def test_put_switches_active_provider(provider_client) -> None:
    client, _ = provider_client
    resp = await client.put(PREFIX, json={"provider": "academic"})
    assert resp.status_code == 200
    assert resp.json()["data"]["active"] == "ACADEMIC"

    # Persisted: a follow-up GET reflects the new active provider.
    follow_up = await client.get(PREFIX)
    assert follow_up.json()["data"]["active"] == "ACADEMIC"


async def test_put_unknown_provider_is_422(provider_client) -> None:
    client, _ = provider_client
    resp = await client.put(PREFIX, json={"provider": "litellm"})
    assert resp.status_code == 422
    assert resp.json()["success"] is False


async def test_put_provider_without_key_is_422(db_engine) -> None:
    # Build a client whose Academic Cloud key is missing.
    factory = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    test_settings = _settings(LLM_API_KEY=None)

    async def override_session() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            yield session

    with (
        patch("app.database.check_db_connection", new=AsyncMock(return_value=True)),
        patch("app.database.init_db", new=AsyncMock()),
        patch("app.database.dispose_engine", new=AsyncMock()),
    ):
        from app.config import get_settings
        from app.database import get_session
        from app.main import create_app

        app = create_app()
        app.dependency_overrides[get_session] = override_session
        app.dependency_overrides[get_settings] = lambda: test_settings

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.put(PREFIX, json={"provider": "ACADEMIC"})
            assert resp.status_code == 422
            assert "no api key" in resp.json()["error"].lower()
