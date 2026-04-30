from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.enums import ActorType, CodebookStatus
from app.models.base import Base, TimestampMixin


class Codebook(Base, TimestampMixin):
    """Version boundary for the induced codebook used in deductive coding."""

    __tablename__ = "codebooks"
    __table_args__ = (
        UniqueConstraint("project_id", "version", name="uq_codebook_project_version"),
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    previous_version_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("codebooks.id", ondelete="SET NULL"), nullable=True, index=True
    )

    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    version: Mapped[int] = mapped_column(Integer())
    status: Mapped[CodebookStatus] = mapped_column(Enum(CodebookStatus, native_enum=False))

    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))
