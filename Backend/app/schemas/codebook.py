import enum
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Union
from uuid import UUID

from pydantic import Field, ValidationInfo, field_validator

from app.schemas.common import BaseSchema


class NodeType(enum.StrEnum):
    THEME = "THEME"
    SUBTHEME = "SUBTHEME"
    CODE = "CODE"

if TYPE_CHECKING:
    from app.models.code import Code
    from app.models.themes import Theme

# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------

MIN_THEMES = 1
MAX_THEMES = 50


class NodeInput(BaseSchema):
    """One node supplied by the researcher (Theme, Subtheme, or Code)."""

    node_type: NodeType = Field(default=NodeType.THEME)
    name: str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., min_length=1)
    parent_name: str | None = Field(default=None, max_length=255)


class CodebookCreateRequest(BaseSchema):
    """Payload for creating a new codebook with its themes in one shot."""

    name: str = Field(..., min_length=1, max_length=255)
    corpus_id: UUID = Field(..., description="Corpus ID to scope this codebook.")
    themes: list[NodeInput] = Field(default_factory=list)
    nodes: list[NodeInput] = Field(default_factory=list)

    @field_validator("nodes", mode="before")
    @classmethod
    def populate_nodes(cls, v: list[NodeInput] | None, info: ValidationInfo) -> list[NodeInput]:
        # Handle backward compatibility where payload has `themes` instead of `nodes`
        if v:
            return v
        return info.data.get("themes") or []

    @field_validator("nodes")
    @classmethod
    def validate_node_count(cls, v: list[NodeInput]) -> list[NodeInput]:
        if not (MIN_THEMES <= len(v) <= MAX_THEMES):
            raise ValueError(
                f"Codebook must have between {MIN_THEMES} and {MAX_THEMES} nodes; "
                f"got {len(v)}."
            )
        for node in v:
            if node.node_type in (NodeType.THEME, NodeType.SUBTHEME):
                if node.parent_name:
                    node.node_type = NodeType.SUBTHEME
                else:
                    node.node_type = NodeType.THEME
        # CSV codebook standard: a CODE must sit under a THEME or SUBTHEME.
        # The DB schema accepts orphan codes (no parent) but they are
        # semantically meaningless in qualitative thematic analysis, so
        # reject them at the contract boundary.
        for node in v:
            if node.node_type == NodeType.CODE and not node.parent_name:
                raise ValueError(
                    f"Code '{node.name}' must have a parent theme or subtheme; "
                    "orphan codes are not allowed."
                )
        return v


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class CodebookSchema(BaseSchema):
    """Flat codebook read-back (used by the existing list endpoint)."""

    id: UUID
    corpus_id: UUID
    name: str
    description: str | None = None
    version: int
    created_by: str


class CodebookGenerateRequest(BaseSchema):
    codebook_name: str = Field(min_length=1, max_length=255)
    corpus_id: UUID
    transcript_document_ids: list[UUID] | None = None

    @field_validator("codebook_name")
    @classmethod
    def normalize_codebook_name(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("codebook_name must not be empty")
        return normalized

    @field_validator("transcript_document_ids")
    @classmethod
    def normalize_transcript_document_ids(cls, values: list[UUID] | None) -> list[UUID] | None:
        if values is None:
            return None
        if not values:
            return None
        return values


class GeneratedCodebookResponse(BaseSchema):
    class PassageFailure(BaseSchema):
        passage_index: int
        passage_excerpt: str
        error: str
        attempts: int

    codebook: CodebookSchema
    transcripts_processed: int
    passages_processed: int
    themes_created: int
    codes_created: int
    passages_failed: int = 0
    failed_passages: list[PassageFailure] = Field(default_factory=list)


JobStatus = Literal["queued", "running", "succeeded", "failed", "cancelled"]


class CodebookGenerationJobCreateRequest(CodebookGenerateRequest):
    pass


class CodebookGenerationJobSchema(BaseSchema):
    id: UUID
    status: JobStatus
    phase: str
    progress_percent: int
    codebook_name: str
    corpus_id: UUID
    transcript_document_ids: list[UUID]
    cancel_requested: bool

    codebook_id: UUID | None = None
    passages_total: int
    passages_done: int
    transcripts_processed: int | None = None
    passages_processed: int | None = None
    themes_created: int | None = None
    codes_created: int | None = None
    error_message: str | None = None

    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None


class CodeInCodebookSchema(BaseSchema):
    """A single persisted code node."""

    id: UUID
    node_type: NodeType = Field(default=NodeType.CODE)
    name: str  # maps from Code.label
    description: str | None = None

    @classmethod
    def from_code(cls, code: "Code") -> "CodeInCodebookSchema":
        return cls(
            id=code.id,
            node_type=NodeType.CODE,
            name=code.label,
            description=code.description,
        )


class ThemeInCodebookSchema(BaseSchema):
    """A single persisted theme node, potentially containing nested children."""

    id: UUID
    node_type: NodeType = Field(default=NodeType.THEME)
    name: str  # maps from Theme.label
    description: str | None = None
    children: list[Union["ThemeInCodebookSchema", CodeInCodebookSchema]] = Field(default_factory=list)

    @classmethod
    def from_theme(cls, theme: "Theme", is_subtheme: bool = False) -> "ThemeInCodebookSchema":
        return cls(
            id=theme.id,
            node_type=NodeType.SUBTHEME if is_subtheme else NodeType.THEME,
            name=theme.label,
            description=theme.description,
            children=[]
        )


class CodebookDetailSchema(CodebookSchema):
    """Full codebook read-back including its themes."""

    themes: list[ThemeInCodebookSchema] = Field(default_factory=list)
    codes: list[CodeInCodebookSchema] = Field(default_factory=list)
