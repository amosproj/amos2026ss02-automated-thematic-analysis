import uuid
from typing import Any

from sqlalchemy import JSON, ForeignKey, Integer, String, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base, TimestampMixin


class DemographicFiles(Base, TimestampMixin):
    """Model for demographic files uploaded."""

    __tablename__ = 'demographic_files'
    __table_args__ = (
        UniqueConstraint("corpus_id", "name", name="uq_demographic_file_corpus_name"),
    )
    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    original_columns: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    corpus_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("corpora.id", ondelete="CASCADE"), index=True
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
    __table_args__ = (
        UniqueConstraint("corpus_id", "interviewee_id", name="uq_demographic_row_corpus_interviewee"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    demographic_file_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("demographic_files.id", ondelete="CASCADE"), index=True
    )
    corpus_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("corpora.id", ondelete="CASCADE"), index=True
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    interviewee_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    file: Mapped["DemographicFiles"] = relationship(
        back_populates="rows",
    )
