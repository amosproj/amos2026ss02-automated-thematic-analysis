from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Integer, String, Text, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

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
    """One source document within a corpus. Only metadata is stored here; text lives in chunks."""

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
    title: Mapped[str] = mapped_column(String(500))
    # Original uploaded filename (after duplicate-collision resolution). NULL for body-ingested docs.
    filename: Mapped[str | None] = mapped_column(String(500), nullable=True)


class CorpusChunk(Base, TimestampMixin):
    """Fixed-size word-window slice of a CorpusDocument, consumed by downstream analysis."""

    __tablename__ = "corpus_chunks"
    # Prevents re-ingesting the same document from producing duplicate chunks.
    __table_args__ = (
        UniqueConstraint("document_id", "chunk_index", name="uq_corpus_chunk_document_index"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    document_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("corpus_documents.id", ondelete="CASCADE"),
        index=True,
    )
    text: Mapped[str] = mapped_column(Text())
    chunk_index: Mapped[int] = mapped_column(Integer())  # zero-based position within the document
