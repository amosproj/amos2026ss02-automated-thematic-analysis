from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import CheckConstraint, Enum, ForeignKey, Integer, JSON, String, Text, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.domain.enums import DocumentStatus, IngestionRunStatus, SourceType
from app.models.base import Base, TimestampMixin


class Corpus(Base, TimestampMixin):
    """Container for source material belonging to a single analytical project."""

    __tablename__ = "corpora"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    project_id: Mapped[uuid.UUID] = mapped_column(Uuid, index=True)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    research_question: Mapped[str | None] = mapped_column(Text(), nullable=True)
    # SQLAlchemy reserves 'metadata' on DeclarativeBase; use extra_metadata → column "metadata"
    extra_metadata: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict)


class CorpusDocument(Base, TimestampMixin):
    """Raw source document ingested into a corpus before chunking."""

    __tablename__ = "corpus_documents"
    __table_args__ = (
        UniqueConstraint("corpus_id", "text_hash", name="uq_corpus_document_text_hash"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    corpus_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("corpora.id", ondelete="CASCADE"), index=True
    )
    external_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    text: Mapped[str] = mapped_column(Text())
    text_hash: Mapped[str] = mapped_column(String(64), index=True)
    word_count: Mapped[int] = mapped_column(Integer())
    source_type: Mapped[SourceType] = mapped_column(Enum(SourceType, native_enum=False), index=True)
    status: Mapped[DocumentStatus] = mapped_column(
        Enum(DocumentStatus, native_enum=False), index=True
    )
    extra_metadata: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict)


class CorpusChunk(Base, TimestampMixin):
__tablename__ = "corpus_chunks"
    __table_args__ = (
        UniqueConstraint("document_id", "chunk_index", name="uq_corpus_chunk_document_index"),
        CheckConstraint("start_word >= 0", name="ck_corpus_chunk_start_word"),
        CheckConstraint("end_word >= start_word", name="ck_corpus_chunk_end_word"),
        CheckConstraint("word_count >= 0", name="ck_corpus_chunk_word_count"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    document_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("corpus_documents.id", ondelete="CASCADE"), index=True
    )
    chunk_index: Mapped[int] = mapped_column(Integer())
    start_word: Mapped[int] = mapped_column(Integer())
    end_word: Mapped[int] = mapped_column(Integer())
    text: Mapped[str] = mapped_column(Text())
    text_hash: Mapped[str] = mapped_column(String(64), index=True)
    word_count: Mapped[int] = mapped_column(Integer())


class IngestionRun(Base, TimestampMixin):
    """Audit record for a single batch ingestion operation."""

    __tablename__ = "ingestion_runs"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    corpus_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("corpora.id", ondelete="CASCADE"), index=True
    )
    source_type: Mapped[SourceType] = mapped_column(
        Enum(SourceType, native_enum=False), index=True
    )
    status: Mapped[IngestionRunStatus] = mapped_column(
        Enum(IngestionRunStatus, native_enum=False), index=True
    )
    filename: Mapped[str | None] = mapped_column(String(500), nullable=True)
    total_documents: Mapped[int] = mapped_column(Integer(), default=0)
    accepted_documents: Mapped[int] = mapped_column(Integer(), default=0)
    rejected_documents: Mapped[int] = mapped_column(Integer(), default=0)
    duplicate_documents: Mapped[int] = mapped_column(Integer(), default=0)
    empty_documents: Mapped[int] = mapped_column(Integer(), default=0)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)
