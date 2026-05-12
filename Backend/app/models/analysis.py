from __future__ import annotations

import uuid

from sqlalchemy import Boolean, Float, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin

class DocumentAnalysis(Base, TimestampMixin):
    """Stores the result of an LLM thematic analysis applying a Codebook to a CorpusDocument."""

    __tablename__ = "document_analysis"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("corpus_documents.id", ondelete="CASCADE"), index=True
    )
    codebook_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("codebooks.id", ondelete="CASCADE"), index=True
    )
    summary: Mapped[str | None] = mapped_column(Text(), nullable=True)
    researcher_notes: Mapped[str | None] = mapped_column(Text(), nullable=True)


class ThemeOccurrence(Base, TimestampMixin):
    """Stores the LLM's assessment of whether a specific theme was present in a document analysis."""

    __tablename__ = "theme_occurrences"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    analysis_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("document_analysis.id", ondelete="CASCADE"), index=True
    )
    theme_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("themes.id", ondelete="CASCADE"), index=True
    )
    is_present: Mapped[bool] = mapped_column(Boolean(), default=False)
    confidence: Mapped[float] = mapped_column(Float(), default=0.0)
    quote: Mapped[str | None] = mapped_column(Text(), nullable=True)
