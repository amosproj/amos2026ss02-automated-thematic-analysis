from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class CodebookGenerationJob(Base, TimestampMixin):
    __tablename__ = "codebook_generation_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    status: Mapped[str] = mapped_column(String(32), index=True, default="queued")
    codebook_name: Mapped[str] = mapped_column(String(255))
    corpus_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), index=True)
    transcript_document_ids_json: Mapped[str] = mapped_column(Text())
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)

    codebook_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    passages_total: Mapped[int] = mapped_column(Integer, default=0)
    passages_done: Mapped[int] = mapped_column(Integer, default=0)
    transcripts_processed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    passages_processed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    themes_created: Mapped[int | None] = mapped_column(Integer, nullable=True)
    codes_created: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
