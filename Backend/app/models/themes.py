from __future__ import annotations

import uuid

from sqlalchemy import Boolean, ForeignKey, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class Theme(Base, TimestampMixin):
    """Simplified theme artifact."""
    """TODO: Unfinished placeholder model; feel free to change whatever you want. Versioning is intentionally not implemented."""

    __tablename__ = "themes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    label: Mapped[str] = mapped_column(String(255), index=True)
    is_active: Mapped[bool] = mapped_column(Boolean(), default=True)


class CodebookThemeRelationship(Base, TimestampMixin):
    """Membership link between a codebook and a theme."""

    __tablename__ = "codebook_theme_relationships"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Link each membership row to an existing codebook and theme;
    # cascading delete removes memberships automatically when a parent is deleted.
    codebook_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    theme_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("themes.id", ondelete="CASCADE"), index=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean(), default=True)
