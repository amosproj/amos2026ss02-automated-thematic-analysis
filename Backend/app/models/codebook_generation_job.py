from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class CodebookGenerationJob(Base, TimestampMixin):
    __tablename__ = "codebook_generation_jobs"

    # Status moves through queued, running, succeeded, failed, or cancelled.
    # Phase stores the current traceable pipeline stage for durable progress
    # reporting across workers and browser sessions.
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
    # Store selected transcript IDs as JSON to keep the job table simple.
    transcript_document_ids_json: Mapped[str] = mapped_column(Text())
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)

    codebook_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    application_run_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)

    # The new traceable pipeline works at document level. The legacy passage
    # counters stay as compatibility aliases for older frontend code/tests.
    documents_total: Mapped[int] = mapped_column(Integer, default=0)
    documents_done: Mapped[int] = mapped_column(Integer, default=0)
    analysis_units_total: Mapped[int] = mapped_column(Integer, default=0)
    analysis_units_done: Mapped[int] = mapped_column(Integer, default=0)
    passages_total: Mapped[int] = mapped_column(Integer, default=0)
    passages_done: Mapped[int] = mapped_column(Integer, default=0)
    transcripts_processed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    passages_processed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quotes_created: Mapped[int | None] = mapped_column(Integer, nullable=True)
    themes_created: Mapped[int | None] = mapped_column(Integer, nullable=True)
    codes_created: Mapped[int | None] = mapped_column(Integer, nullable=True)
    documents_coded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    documents_failed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Failed jobs store the exception message; successful partial runs store
    # structured JSON describing passages skipped after repeated parser errors.
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)
    provenance_json: Mapped[str | None] = mapped_column(Text(), nullable=True)
    action_log_json: Mapped[str | None] = mapped_column(Text(), nullable=True)
    research_query: Mapped[str | None] = mapped_column(Text(), nullable=True)
    researcher_topics: Mapped[str | None] = mapped_column(Text(), nullable=True)
    max_refinement_rounds: Mapped[int] = mapped_column(Integer, default=5)
    apply_after_generation: Mapped[bool] = mapped_column(Boolean, default=True)

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
