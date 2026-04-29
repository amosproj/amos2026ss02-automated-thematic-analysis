from __future__ import annotations

from enum import StrEnum

from sqlalchemy import Enum, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class ActorType(StrEnum):
    """Actor that generated or edited an artifact/link."""

    LLM = "llm"
    HUMAN = "human"
    SYSTEM = "system"


class CodebookStatus(StrEnum):
    """Lifecycle state of a versioned codebook snapshot."""

    DRAFT = "draft"
    ACTIVE = "active"
    FROZEN = "frozen"
    ARCHIVED = "archived"


class NodeStatus(StrEnum):
    """Lifecycle state for code/theme artifacts during refinement."""

    CANDIDATE = "candidate"
    ACTIVE = "active"
    MERGED = "merged"
    DEPRECATED = "deprecated"
    DELETED = "deleted"


class RelationshipStatus(StrEnum):
    """Lifecycle state for typed relationship edges."""

    ACTIVE = "active"
    REMOVED = "removed"


class Codebook(Base, TimestampMixin):
    """Version boundary for the induced codebook used in deductive coding."""

    __tablename__ = "codebooks"
    __table_args__ = (
        UniqueConstraint("project_id", "version", name="uq_codebook_project_version"),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    previous_version_id: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("codebooks.id", ondelete="SET NULL"), nullable=True, index=True
    )

    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    research_question: Mapped[str] = mapped_column(Text())
    version: Mapped[int] = mapped_column(Integer())
    status: Mapped[CodebookStatus] = mapped_column(Enum(CodebookStatus, native_enum=False))

    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))
