import os
import uuid as _uuid
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
    # Use a named in-memory database with shared cache so that multiple
    # concurrent connections (e.g. main session + background job runner
    # + progress/cancel callbacks) all see the *same* database while each
    # owning an independent transaction.  A plain :memory: URL with
    # StaticPool forces every session onto a single shared connection,
    # causing transaction conflicts when the async job runner opens its
    # own sessions.
    db_name = f"test_{_uuid.uuid4().hex[:8]}"
    url = f"sqlite+aiosqlite:///file:{db_name}?mode=memory&cache=shared&uri=true"
    engine = create_async_engine(
        url,
        echo=False,
        connect_args={"check_same_thread": False, "timeout": 10},
    )
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
        patch("app.database.check_db_connection", new=AsyncMock(return_value=True)),
        patch("app.database.init_db", new=AsyncMock()),
        patch("app.database.dispose_engine", new=AsyncMock()),
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
