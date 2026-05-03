from __future__ import annotations

import uuid

from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.models.codebook import Codebook
from app.models.themes import CodebookThemeRelationship, Theme
from app.schemas.codebook import CodebookSchema
from app.schemas.theme import ThemeSchema


def _model_columns(model: type[object]) -> set[str]:
    # TimestampMixin columns are internal metadata fields, not schema payload fields.
    return {column.name for column in model.__table__.columns if column.name not in {"created_at", "updated_at"}}


def test_postgres_uuid_columns_configured() -> None:
    assert isinstance(Codebook.__table__.c.id.type, PG_UUID)
    assert isinstance(Theme.__table__.c.id.type, PG_UUID)
    assert isinstance(CodebookThemeRelationship.__table__.c.id.type, PG_UUID)
    assert isinstance(CodebookThemeRelationship.__table__.c.codebook_id.type, PG_UUID)
    assert isinstance(CodebookThemeRelationship.__table__.c.theme_id.type, PG_UUID)


def test_codebook_theme_relationship_foreign_keys() -> None:
    foreign_keys = {fk.parent.name: fk for fk in CodebookThemeRelationship.__table__.foreign_keys}

    codebook_fk = foreign_keys["codebook_id"]
    theme_fk = foreign_keys["theme_id"]

    assert codebook_fk.column.table.name == "codebooks"
    assert codebook_fk.column.name == "id"
    assert codebook_fk.ondelete == "CASCADE"

    assert theme_fk.column.table.name == "themes"
    assert theme_fk.column.name == "id"
    assert theme_fk.ondelete == "CASCADE"


def test_schema_fields_match_sqlalchemy_models() -> None:
    assert set(CodebookSchema.model_fields) == _model_columns(Codebook)
    assert set(ThemeSchema.model_fields) == _model_columns(Theme)


def test_pydantic_models_validate_from_sqlalchemy_objects() -> None:
    codebook = Codebook(
        id=uuid.uuid4(),
        project_id="project-a",
        name="Codebook A",
        description="placeholder",
        version=1,
        created_by="human",
    )
    codebook_schema = CodebookSchema.model_validate(codebook)
    assert codebook_schema.id == codebook.id
    assert codebook_schema.project_id == codebook.project_id
    assert codebook_schema.name == codebook.name

    theme = Theme(id=uuid.uuid4(), label="Trust", is_active=True)
    theme_schema = ThemeSchema.model_validate(theme)
    assert theme_schema.id == theme.id
    assert theme_schema.label == theme.label
    assert theme_schema.is_active is True
