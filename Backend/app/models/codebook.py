from __future__ import annotations

import uuid

from sqlalchemy import Enum, ForeignKey, Integer, String, Text, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.enums import ActorType, CodebookStatus
from app.models.base import Base, TimestampMixin


class Codebook(Base, TimestampMixin):
    """Version boundary for the induced codebook used in deductive coding."""

    __tablename__ = "codebooks"
    __table_args__ = (
        UniqueConstraint("project_id", "version", name="uq_codebook_project_version"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    project_id: Mapped[uuid.UUID] = mapped_column(Uuid, index=True)
    previous_version_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid, ForeignKey("codebooks.id", ondelete="SET NULL"), nullable=True, index=True
    )

    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    research_question: Mapped[str] = mapped_column(Text())
    version: Mapped[int] = mapped_column(Integer())
    status: Mapped[CodebookStatus] = mapped_column(Enum(CodebookStatus, native_enum=False))

    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))
