from uuid import UUID

from pydantic import Field, field_validator

from app.schemas.common import BaseSchema

# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------

MIN_THEMES = 1
MAX_THEMES = 50


class ThemeInput(BaseSchema):
    """One theme supplied by the researcher — used in both CSV upload and manual entry."""

    name: str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., min_length=1)


class CodebookCreateRequest(BaseSchema):
    """Payload for creating a new codebook with its themes in one shot."""

    name: str = Field(..., min_length=1, max_length=255)
    project_id: str = Field(..., min_length=1, max_length=64)
    themes: list[ThemeInput]

    @field_validator("themes")
    @classmethod
    def validate_theme_count(cls, v: list[ThemeInput]) -> list[ThemeInput]:
        if not (MIN_THEMES <= len(v) <= MAX_THEMES):
            raise ValueError(
                f"Codebook must have between {MIN_THEMES} and {MAX_THEMES} themes; "
                f"got {len(v)}."
            )
        return v


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class CodebookSchema(BaseSchema):
    """Flat codebook read-back (used by the existing list endpoint)."""

    id: UUID
    project_id: str
    name: str
    description: str | None = None
    version: int
    created_by: str


class ThemeInCodebookSchema(BaseSchema):
    """A single persisted theme as returned inside a CodebookDetailSchema."""

    id: UUID
    name: str  # maps from Theme.label
    description: str | None = None

    @classmethod
    def from_theme(cls, theme) -> "ThemeInCodebookSchema":
        return cls(id=theme.id, name=theme.label, description=theme.description)


class CodebookDetailSchema(CodebookSchema):
    """Full codebook read-back including its themes."""

    themes: list[ThemeInCodebookSchema] = Field(default_factory=list)
