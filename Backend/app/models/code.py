from __future__ import annotations

import uuid

from sqlalchemy import Boolean, ForeignKey, String, Text, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class Code(Base, TimestampMixin):
    """Simplified code artifact."""

    __tablename__ = "codes"
    __table_args__ = (
        UniqueConstraint("codebook_id", "label", name="uq_code_codebook_label"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    codebook_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    label: Mapped[str] = mapped_column(String(255), index=True)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean(), default=True)


class CodebookCodeRelationship(Base, TimestampMixin):
    """Membership link between a codebook and a code."""

    __tablename__ = "codebook_code_relationships"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    codebook_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    code_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codes.id", ondelete="CASCADE"), index=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean(), default=True)


class ThemeCodeRelationship(Base, TimestampMixin):
    """Membership link between a theme and a code."""

    __tablename__ = "theme_code_relationships"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    codebook_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    theme_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("themes.id", ondelete="CASCADE"), index=True
    )
    code_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codes.id", ondelete="CASCADE"), index=True
    )
    is_active: Mapped[bool] = mapped_column(Boolean(), default=True)
