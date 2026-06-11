from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class CodebookApplicationRun(Base, TimestampMixin):
    """One autonomous application of a codebook to a selected transcript set."""

    __tablename__ = "codebook_application_runs"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    corpus_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("corpora.id", ondelete="CASCADE"), index=True
    )
    codebook_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    status: Mapped[str] = mapped_column(String(32), index=True, default="running")
    documents_total: Mapped[int] = mapped_column(Integer, default=0)
    documents_coded: Mapped[int] = mapped_column(Integer, default=0)
    documents_failed: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class DocumentCoding(Base, TimestampMixin):
    """Coding result for one transcript within a codebook application run."""

    __tablename__ = "document_codings"
    __table_args__ = (
        UniqueConstraint("application_run_id", "document_id", name="uq_document_coding_run_document"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    application_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codebook_application_runs.id", ondelete="CASCADE"), index=True
    )
    document_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("corpus_documents.id", ondelete="CASCADE"), index=True
    )
    codebook_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    status: Mapped[str] = mapped_column(String(32), index=True, default="coded")
    summary: Mapped[str | None] = mapped_column(Text(), nullable=True)
    researcher_notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)


class ThemeAssignment(Base, TimestampMixin):
    """Theme-level presence assessment for one coded transcript."""

    __tablename__ = "theme_assignments"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_coding_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("document_codings.id", ondelete="CASCADE"), index=True
    )
    theme_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("themes.id", ondelete="CASCADE"), index=True
    )
    is_present: Mapped[bool] = mapped_column(Boolean(), default=False)
    confidence: Mapped[float] = mapped_column(Float(), default=0.0)
    quote: Mapped[str | None] = mapped_column(Text(), nullable=True)
    start_char: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_char: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quote_match_status: Mapped[str | None] = mapped_column(String(32), nullable=True)


class CodeAssignment(Base, TimestampMixin):
    """Span-level code assignment for one coded transcript."""

    __tablename__ = "code_assignments"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_coding_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("document_codings.id", ondelete="CASCADE"), index=True
    )
    code_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codes.id", ondelete="CASCADE"), index=True
    )
    theme_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("themes.id", ondelete="SET NULL"), nullable=True, index=True
    )
    quote: Mapped[str] = mapped_column(Text())
    start_char: Mapped[int | None] = mapped_column(Integer, nullable=True)
    end_char: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quote_match_status: Mapped[str] = mapped_column(String(32), default="not_found")
    confidence: Mapped[float] = mapped_column(Float(), default=0.0)
    rationale: Mapped[str | None] = mapped_column(Text(), nullable=True)


class CodebookApplicationJob(Base, TimestampMixin):
    """Background job for applying a codebook to transcripts."""

    __tablename__ = "codebook_application_jobs"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    phase: Mapped[str] = mapped_column(String(64), default="queued")
    corpus_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), index=True)
    codebook_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), index=True)
    transcript_document_ids_json: Mapped[str] = mapped_column(Text())
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    application_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("codebook_application_runs.id", ondelete="SET NULL"), nullable=True
    )
    documents_total: Mapped[int] = mapped_column(Integer, default=0)
    documents_done: Mapped[int] = mapped_column(Integer, default=0)
    documents_coded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    documents_failed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)

