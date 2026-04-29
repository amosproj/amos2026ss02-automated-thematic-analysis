from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    Enum,
    Float,
    ForeignKey,
    Index,
    String,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.enums import (
    ActorType,
    CodeRelationshipType,
    CodebookCodeRelationshipType,
    NodeStatus,
    RelationshipStatus,
)
from app.models.base import Base, TimestampMixin


class Code(Base, TimestampMixin):
    """Open-coding level analytical concept distilled from source evidence."""

    __tablename__ = "codes"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    label: Mapped[str] = mapped_column(String(255), index=True)
    description: Mapped[str] = mapped_column(Text())
    status: Mapped[NodeStatus] = mapped_column(Enum(NodeStatus, native_enum=False), index=True)

    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))


class CodebookCodeRelationship(Base, TimestampMixin):
    """Membership edge linking a codebook version to an active code artifact."""

    __tablename__ = "codebook_code_relationships"
    __table_args__ = (
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_codebook_code_rel_confidence_range",
        ),
        Index(
            "uq_codebook_code_rel_active",
            "codebook_id",
            "code_id",
            "relationship_type",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    codebook_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    code_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("codes.id", ondelete="CASCADE"), index=True
    )
    relationship_type: Mapped[CodebookCodeRelationshipType] = mapped_column(
        Enum(CodebookCodeRelationshipType, native_enum=False),
        default=CodebookCodeRelationshipType.CONTAINS,
    )
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    provenance: Mapped[str | None] = mapped_column(Text(), nullable=True)
    status: Mapped[RelationshipStatus] = mapped_column(
        Enum(RelationshipStatus, native_enum=False), default=RelationshipStatus.ACTIVE
    )
    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))


class CodeRelationship(Base, TimestampMixin):
    """Typed semantic edge between two codes (e.g., subordinate/equivalent/orthogonal)."""

    __tablename__ = "code_relationships"
    __table_args__ = (
        CheckConstraint("source_code_id <> target_code_id", name="ck_code_rel_no_self_link"),
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_code_rel_confidence_range",
        ),
        Index(
            "uq_code_rel_active",
            "codebook_id",
            "source_code_id",
            "target_code_id",
            "relationship_type",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    codebook_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    source_code_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("codes.id", ondelete="CASCADE"), index=True
    )
    target_code_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("codes.id", ondelete="CASCADE"), index=True
    )
    relationship_type: Mapped[CodeRelationshipType] = mapped_column(
        Enum(CodeRelationshipType, native_enum=False), index=True
    )
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    provenance: Mapped[str | None] = mapped_column(Text(), nullable=True)
    status: Mapped[RelationshipStatus] = mapped_column(
        Enum(RelationshipStatus, native_enum=False), default=RelationshipStatus.ACTIVE
    )
    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))
