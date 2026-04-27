from collections.abc import AsyncGenerator
from functools import lru_cache

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings


@lru_cache
def _get_engine():
    settings = get_settings()
    return create_async_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        echo=settings.APP_DEBUG,
    )


def _get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        _get_engine(),
        expire_on_commit=False,
        class_=AsyncSession,
    )


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    factory = _get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


async def check_db_connection() -> bool:
    try:
        factory = _get_session_factory()
        async with factory() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def dispose_engine() -> None:
    engine = _get_engine()
    await engine.dispose()
    _get_engine.cache_clear()
