from __future__ import annotations

from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class AppSetting(Base, TimestampMixin):
    """Single-row-per-key store for mutable, app-wide settings.

    Used for runtime configuration that the UI can change without a redeploy,
    such as the active LLM provider. Keeping it as a generic key/value table
    means new global toggles don't each need their own table or migration —
    relevant because the schema is created via ``create_all`` (no Alembic).
    """

    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(Text())


# Well-known setting keys.
ACTIVE_LLM_PROVIDER_KEY = "active_llm_provider"
