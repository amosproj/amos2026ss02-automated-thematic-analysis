from __future__ import annotations

import uuid

from sqlalchemy import Uuid as PG_UUID

from app.models.code import Code
from app.models.codebook import Codebook
from app.models.themes import CodebookThemeRelationship, Theme, ThemeHierarchyRelationship
from app.schemas.codebook import CodebookSchema
from app.schemas.theme import ThemeSchema


def _model_columns(model: type[object]) -> set[str]:
    # TimestampMixin columns are internal metadata fields, not schema payload fields.
    return {column.name for column in model.__table__.columns if column.name not in {"created_at", "updated_at"}}


def test_postgres_uuid_columns_configured() -> None:
    assert isinstance(Codebook.__table__.c.id.type, PG_UUID)
    assert isinstance(Code.__table__.c.id.type, PG_UUID)
    assert isinstance(Code.__table__.c.codebook_id.type, PG_UUID)
    assert isinstance(Theme.__table__.c.id.type, PG_UUID)
    assert isinstance(Theme.__table__.c.codebook_id.type, PG_UUID)
    assert isinstance(CodebookThemeRelationship.__table__.c.id.type, PG_UUID)
    assert isinstance(CodebookThemeRelationship.__table__.c.codebook_id.type, PG_UUID)
    assert isinstance(CodebookThemeRelationship.__table__.c.theme_id.type, PG_UUID)
    assert isinstance(ThemeHierarchyRelationship.__table__.c.id.type, PG_UUID)
    assert isinstance(ThemeHierarchyRelationship.__table__.c.codebook_id.type, PG_UUID)
    assert isinstance(ThemeHierarchyRelationship.__table__.c.parent_theme_id.type, PG_UUID)
    assert isinstance(ThemeHierarchyRelationship.__table__.c.child_theme_id.type, PG_UUID)


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


def test_theme_hierarchy_relationship_foreign_keys() -> None:
    foreign_keys = {fk.parent.name: fk for fk in ThemeHierarchyRelationship.__table__.foreign_keys}

    codebook_fk = foreign_keys["codebook_id"]
    parent_fk = foreign_keys["parent_theme_id"]
    child_fk = foreign_keys["child_theme_id"]

    assert codebook_fk.column.table.name == "codebooks"
    assert codebook_fk.column.name == "id"
    assert codebook_fk.ondelete == "CASCADE"

    assert parent_fk.column.table.name == "themes"
    assert parent_fk.column.name == "id"
    assert parent_fk.ondelete == "CASCADE"

    assert child_fk.column.table.name == "themes"
    assert child_fk.column.name == "id"
    assert child_fk.ondelete == "CASCADE"


def test_theme_and_code_labels_are_unique_within_codebook() -> None:
    theme_unique_constraints = {constraint.name for constraint in Theme.__table__.constraints}
    code_unique_constraints = {constraint.name for constraint in Code.__table__.constraints}

    assert "uq_theme_codebook_label" in theme_unique_constraints
    assert "uq_code_codebook_label" in code_unique_constraints


def test_schema_fields_match_sqlalchemy_models() -> None:
    schema_fields = set(CodebookSchema.model_fields) - {"started_at", "finished_at"}
    assert schema_fields == _model_columns(Codebook)
    assert set(ThemeSchema.model_fields) == _model_columns(Theme)


def test_pydantic_models_validate_from_sqlalchemy_objects() -> None:
    codebook = Codebook(
        id=uuid.uuid4(),
        corpus_id=uuid.UUID("3d756af1-9eb4-96de-570c-ebd361d87202"),
        name="Codebook A",
        description="placeholder",
        version=1,
        created_by="human",
    )
    codebook_schema = CodebookSchema.model_validate(codebook)
    assert codebook_schema.id == codebook.id
    assert codebook_schema.corpus_id == codebook.corpus_id
    assert codebook_schema.name == codebook.name

    theme = Theme(
        id=uuid.uuid4(),
        codebook_id=uuid.uuid4(),
        label="Trust",
        is_active=True,
    )
    theme_schema = ThemeSchema.model_validate(theme)
    assert theme_schema.id == theme.id
    assert theme_schema.codebook_id == theme.codebook_id
    assert theme_schema.label == theme.label
    assert theme_schema.is_active is True
