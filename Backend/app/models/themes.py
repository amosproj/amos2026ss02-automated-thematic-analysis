from __future__ import annotations

from enum import StrEnum

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

from app.models.base import Base, TimestampMixin
from app.models.codebook import ActorType, NodeStatus, RelationshipStatus


class ThemeLevel(StrEnum):
    """Hierarchy level for thematic artifacts."""

    THEME = "theme"
    SUBTHEME = "subtheme"


class ThemeRelationshipType(StrEnum):
    """Allowed structural/semantic relations between themes/subthemes."""

    CHILD_OF = "child_of"
    EQUIVALENT_TO = "equivalent_to"
    RELATED_TO = "related_to"


class CodeThemeRelationshipType(StrEnum):
    """Cross-level relation labels from codes to (sub)themes."""

    MEMBER_OF = "member_of"
    SUPPORTS = "supports"


class CodebookThemeRelationshipType(StrEnum):
    """Membership relation labels between a codebook and a theme."""

    CONTAINS = "contains"


class Theme(Base, TimestampMixin):
    """Higher-order synthesis artifact representing a theme or subtheme."""

    __tablename__ = "themes"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    label: Mapped[str] = mapped_column(String(255), index=True)
    description: Mapped[str] = mapped_column(Text())
    level: Mapped[ThemeLevel] = mapped_column(Enum(ThemeLevel, native_enum=False), index=True)
    status: Mapped[NodeStatus] = mapped_column(Enum(NodeStatus, native_enum=False), index=True)

    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))


class CodebookThemeRelationship(Base, TimestampMixin):
    """Membership edge linking a codebook version to a theme/subtheme artifact."""

    __tablename__ = "codebook_theme_relationships"
    __table_args__ = (
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_codebook_theme_rel_confidence_range",
        ),
        Index(
            "uq_codebook_theme_rel_active",
            "codebook_id",
            "theme_id",
            "relationship_type",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    codebook_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    theme_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("themes.id", ondelete="CASCADE"), index=True
    )
    relationship_type: Mapped[CodebookThemeRelationshipType] = mapped_column(
        Enum(CodebookThemeRelationshipType, native_enum=False),
        default=CodebookThemeRelationshipType.CONTAINS,
    )
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    provenance: Mapped[str | None] = mapped_column(Text(), nullable=True)
    status: Mapped[RelationshipStatus] = mapped_column(
        Enum(RelationshipStatus, native_enum=False), default=RelationshipStatus.ACTIVE
    )
    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))


class ThemeRelationship(Base, TimestampMixin):
    """Typed structural/semantic edge between two thematic artifacts."""

    __tablename__ = "theme_relationships"
    __table_args__ = (
        CheckConstraint("source_theme_id <> target_theme_id", name="ck_theme_rel_no_self_link"),
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_theme_rel_confidence_range",
        ),
        Index(
            "uq_theme_rel_active",
            "codebook_id",
            "source_theme_id",
            "target_theme_id",
            "relationship_type",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    codebook_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    source_theme_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("themes.id", ondelete="CASCADE"), index=True
    )
    target_theme_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("themes.id", ondelete="CASCADE"), index=True
    )
    relationship_type: Mapped[ThemeRelationshipType] = mapped_column(
        Enum(ThemeRelationshipType, native_enum=False), index=True
    )
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    provenance: Mapped[str | None] = mapped_column(Text(), nullable=True)
    status: Mapped[RelationshipStatus] = mapped_column(
        Enum(RelationshipStatus, native_enum=False), default=RelationshipStatus.ACTIVE
    )
    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))


class CodeThemeRelationship(Base, TimestampMixin):
    """Cross-level edge linking a code to a theme/subtheme in the hierarchy."""

    __tablename__ = "code_theme_relationships"
    __table_args__ = (
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_code_theme_rel_confidence_range",
        ),
        Index(
            "uq_code_theme_rel_active",
            "codebook_id",
            "code_id",
            "theme_id",
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
    theme_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("themes.id", ondelete="CASCADE"), index=True
    )
    relationship_type: Mapped[CodeThemeRelationshipType] = mapped_column(
        Enum(CodeThemeRelationshipType, native_enum=False),
        default=CodeThemeRelationshipType.MEMBER_OF,
    )
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    provenance: Mapped[str | None] = mapped_column(Text(), nullable=True)
    status: Mapped[RelationshipStatus] = mapped_column(
        Enum(RelationshipStatus, native_enum=False), default=RelationshipStatus.ACTIVE
    )
    created_by: Mapped[ActorType] = mapped_column(Enum(ActorType, native_enum=False))
