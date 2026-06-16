from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.models.demographic import DemographicRow

from sqlalchemy import ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class Corpus(Base, TimestampMixin):
    """Named collection of documents belonging to one project."""

    __tablename__ = "corpora"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    # Which project this corpus belongs to.
    # TODO: Only placeholder for now. add Project Data Structure and wire correctly into Corpus
    project_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        index=True,
    )
    name: Mapped[str] = mapped_column(String(255))


class CorpusDocument(Base, TimestampMixin):
    """One source document within a corpus."""

    __tablename__ = "corpus_documents"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    corpus_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("corpora.id", ondelete="CASCADE"),
        index=True,
    )
    demographic_row_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("demographic_row.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    demographic_row: Mapped[DemographicRow] = relationship()

    title: Mapped[str] = mapped_column(String(500))
    # Original uploaded filename (after duplicate-collision resolution). NULL for body-ingested docs.
    filename: Mapped[str | None] = mapped_column(String(500), nullable=True)
    content: Mapped[str] = mapped_column(Text())

    @property
    def demographic_data(self) -> dict[str, Any] | None:
        return self.demographic_row.data if self.demographic_row else None

