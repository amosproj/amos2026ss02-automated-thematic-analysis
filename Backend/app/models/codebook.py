from __future__ import annotations

import uuid

from sqlalchemy import ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class Codebook(Base, TimestampMixin):
    """TODO: Unfinished placeholder model; feel free to change whatever you want.
    Codebook versioning is intentionally not implemented."""

    # Idea: (version, id): unique together, so that we can have multiple versions of the same codebook coexisting in
    # the database same as in the papers. This is a potential future direction.
    # project_id is for grouping codebooks by the same lineage.
    __tablename__ = "codebooks"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    corpus_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("corpora.id", ondelete="CASCADE"), index=True)

    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    version: Mapped[int] = mapped_column(Integer())
    created_by: Mapped[str] = mapped_column(String(64))
    # Copied from the generation job so the query remains accessible after the job record is gone.
    research_query: Mapped[str | None] = mapped_column(Text(), nullable=True)
