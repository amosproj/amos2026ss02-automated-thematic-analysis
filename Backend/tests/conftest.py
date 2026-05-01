import os
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Must be set before any app module is imported so Settings() can validate.
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

from app.config import Settings  # noqa: E402
from app.models import Base  # noqa: E402

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture
async def db_engine():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine) -> AsyncGenerator[AsyncSession, None]:
    factory = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)
    async with factory() as session:
        yield session


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        DATABASE_URL=TEST_DB_URL,
        INGESTION_CHUNK_SIZE_WORDS=10,
        INGESTION_CHUNK_OVERLAP_WORDS=2,
    )


@pytest_asyncio.fixture
async def client(db_engine) -> AsyncGenerator[AsyncClient, None]:
    """Full-stack test client with DB overridden to in-memory SQLite."""
    factory = async_sessionmaker(db_engine, expire_on_commit=False, class_=AsyncSession)

    async def override_session() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise

    # Patch startup/shutdown so the app doesn't try to connect to a real DB
    with (
        patch("app.main.check_db_connection", new=AsyncMock(return_value=True)),
        patch("app.main.init_db", new=AsyncMock()),
        patch("app.main.dispose_engine", new=AsyncMock()),
    ):
        # Import here so patches above are in place before create_app runs
        from app.database import get_session
        from app.main import create_app

        app = create_app()
        app.dependency_overrides[get_session] = override_session

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            yield c
