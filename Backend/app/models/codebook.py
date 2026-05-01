from __future__ import annotations

import uuid

from sqlalchemy import Integer, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin


class Codebook(Base, TimestampMixin):
    """TODO: Unfinished placeholder model; feel free to change whatever you want.
    Codebook versioning is intentionally not implemented."""

    # Idea: (version, id): unique together, so that we can have multiple versions of the same codebook coexisting in
    # the database same as in the papers. This is a potential future direction.
    # project_id is for grouping codebooks by the same lineage.
    __tablename__ = "codebooks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    project_id: Mapped[str] = mapped_column(String(64))

    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    version: Mapped[int] = mapped_column(Integer())
    created_by: Mapped[str] = mapped_column(String(64))
