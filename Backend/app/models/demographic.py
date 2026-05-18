import datetime
import uuid
from typing import Any

from sqlalchemy import Boolean, ForeignKey, String, Text, DateTime, func, Integer
from sqlalchemy.dialects.postgresql import UUID, JSONB

from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, TimestampMixin


class DemographicFiles(Base, TimestampMixin):
    """Model for demographic files uploaded."""

    __tablename__ = 'demographic_files'
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    original_columns: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    corpus_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("corpora.id", ondelete="CASCADE"), index=True
    )

    rows: Mapped[list["DemographicRow"]] = relationship(
        back_populates="file",
        cascade="all, delete-orphan",
    )

class DemographicRow(Base):
    """ Represents one row of demographic data, linked to a DemographicFile. The "data" field is a JSON blob containing
    the actual demographic values for that row, with keys corresponding to the original column names from the uploaded
    file."""

    __tablename__ = 'demographic_row'
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    demographic_file_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("demographic_files.id", ondelete="CASCADE"), index=True
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    file: Mapped["DemographicFiles"] = relationship(
        back_populates="rows",
    )
    corpus_document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("corpus_documents.id", ondelete="CASCADE"), index=True
    )
