from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class TraceableAnalysisJob(Base, TimestampMixin):
    """Background job for experimental quote-grounded generation plus application."""

    __tablename__ = "traceable_analysis_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    phase: Mapped[str] = mapped_column(String(64), default="queued")
    codebook_name: Mapped[str] = mapped_column(String(255))
    analysis_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    custom_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    corpus_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), index=True)
    transcript_document_ids_json: Mapped[str] = mapped_column(Text())
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)

    codebook_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    application_run_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)

    documents_total: Mapped[int] = mapped_column(Integer, default=0)
    documents_done: Mapped[int] = mapped_column(Integer, default=0)
    analysis_units_total: Mapped[int] = mapped_column(Integer, default=0)
    analysis_units_done: Mapped[int] = mapped_column(Integer, default=0)
    quotes_created: Mapped[int | None] = mapped_column(Integer, nullable=True)
    codes_created: Mapped[int | None] = mapped_column(Integer, nullable=True)
    themes_created: Mapped[int | None] = mapped_column(Integer, nullable=True)
    documents_coded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    documents_failed: Mapped[int | None] = mapped_column(Integer, nullable=True)

    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)
    provenance_json: Mapped[str | None] = mapped_column(Text(), nullable=True)
    action_log_json: Mapped[str | None] = mapped_column(Text(), nullable=True)
    research_query: Mapped[str | None] = mapped_column(Text(), nullable=True)
    researcher_topics: Mapped[str | None] = mapped_column(Text(), nullable=True)
    max_refinement_rounds: Mapped[int] = mapped_column(Integer, default=1)

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
