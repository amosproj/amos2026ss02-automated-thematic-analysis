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
    corpus_document_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("corpus_documents.id", ondelete="CASCADE"), index=True
    )
    original_columns: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(), nullable=False, server_default=func.now())

    rows: Mapped[list["DemographicRows"]] = relationship(
        back_populates="file",
        cascade="all, delete-orphan",
    )

class DemographicRows(Base):
    """ Represents one row of demographic data, linked to a DemographicFile. The "data" field is a JSON blob containing
    the actual demographic values for that row, with keys corresponding to the original column names from the uploaded
    file."""

    __tablename__ = 'demographic_rows'
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    demographic_file_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("demographic_files.id", ondelete="CASCADE"), index=True
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    file: Mapped["DemographicFiles"] = relationship(
        back_populates="rows",
    )