from app.models.base import Base, IdMixin, TimestampMixin

# Import all ORM models here so Alembic autogenerate can detect them.
# Example: from app.models.corpus import Corpus

__all__ = ["Base", "IdMixin", "TimestampMixin"]
