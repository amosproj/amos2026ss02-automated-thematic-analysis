import enum
from datetime import datetime
from typing import TYPE_CHECKING, Literal, Union
from uuid import UUID

from pydantic import Field, ValidationInfo, field_validator, model_validator

from app.schemas.common import BaseSchema
from app.utils.sanitize import sanitize_research_query

_QUERY_MIN = 10
_QUERY_MAX = 500


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
    research_query: str | None = None
    researcher_topics: str | None = None
    llm_tokens_input: int | None = None
    llm_tokens_output: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class CodebookGenerateRequest(BaseSchema):
    codebook_name: str = Field(min_length=1, max_length=255)
    analysis_name: str | None = Field(
        default=None,
        max_length=255,
        description="Optional name for the application run created after generation.",
    )
    custom_id: str | None = Field(
        default=None,
        max_length=255,
        description="Optional external identifier for the application run.",
    )
    corpus_id: UUID
    transcript_document_ids: list[UUID] | None = None
    transcript_sample_size: int | None = Field(
        default=None,
        gt=0,
        description=(
            "Randomly sample this many transcripts from the corpus to use for "
            "generation, instead of using every transcript. Reduces token usage "
            "for large corpora. Mutually exclusive with transcript_document_ids "
            "— to use specific transcripts, create a corpus containing only them."
        ),
    )
    research_query: str | None = Field(
        default=None,
        max_length=_QUERY_MAX,
        description="Optional free-text research question guiding thematic analysis (up to 500 characters).",
    )
    researcher_topics: str | None = Field(
        default=None,
        max_length=_QUERY_MAX,
        description="Optional comma-separated topics the researcher wants the analysis to cover.",
    )
    max_refinement_rounds: int = Field(
        default=5,
        ge=0,
        le=10,
        description="Maximum traceable reviewer/refinement rounds before selecting the best codebook iteration.",
    )
    apply_after_generation: bool = Field(
        default=True,
        description="Apply the generated codebook to the selected transcripts in the same job.",
    )

    @field_validator("codebook_name")
    @classmethod
    def normalize_codebook_name(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("codebook_name must not be empty")
        return normalized

    @field_validator("analysis_name", "custom_id")
    @classmethod
    def normalize_optional_label(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        return normalized or None

    @field_validator("transcript_document_ids")
    @classmethod
    def normalize_transcript_document_ids(cls, values: list[UUID] | None) -> list[UUID] | None:
        if not values:
            return None
        ordered_unique: list[UUID] = []
        seen: set[UUID] = set()
        for document_id in values:
            if document_id in seen:
                continue
            seen.add(document_id)
            ordered_unique.append(document_id)
        return ordered_unique

    @field_validator("research_query", mode="before")
    @classmethod
    def sanitize_and_validate_query(cls, value: object) -> str | None:
        # Run before Pydantic's own str processing (including str_strip_whitespace)
        # so we can distinguish "user typed only spaces" from "field was empty/omitted".
        # Optional: None and "" are accepted as "no research question".
        # But once the researcher types something it must be a real query.
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            return value  # type: ignore[return-value]  # let Pydantic raise a type error
        if not value.strip():
            raise ValueError("research_query must not be empty or whitespace only.")
        cleaned = sanitize_research_query(value)
        if len(cleaned) < _QUERY_MIN:
            raise ValueError(
                f"research_query must be at least {_QUERY_MIN} characters (got {len(cleaned)})."
            )
        if len(cleaned) > _QUERY_MAX:
            raise ValueError(
                f"research_query must be at most {_QUERY_MAX} characters (got {len(cleaned)})."
            )
        return cleaned

    @field_validator("researcher_topics")
    @classmethod
    def sanitize_and_validate_topics(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = sanitize_research_query(value)
        if not cleaned:
            return None
        if len(cleaned) > _QUERY_MAX:
            raise ValueError(
                f"researcher_topics must be at most {_QUERY_MAX} characters (got {len(cleaned)})."
            )
        return cleaned

    @model_validator(mode="after")
    def validate_transcript_selection(self) -> "CodebookGenerateRequest":
        if self.transcript_document_ids and self.transcript_sample_size:
            raise ValueError(
                "Provide either transcript_document_ids or transcript_sample_size, not both."
            )
        return self


class GeneratedCodebookResponse(BaseSchema):
    class PassageFailure(BaseSchema):
        passage_index: int
        passage_excerpt: str
        error: str
        attempts: int

    codebook: CodebookSchema
    application_run_id: UUID | None = None
    transcripts_processed: int
    passages_processed: int
    themes_created: int
    codes_created: int
    documents_coded: int | None = None
    documents_failed: int | None = None
    quotes_created: int | None = None
    provenance: dict[str, object] | None = None
    action_log: list[dict[str, object]] = Field(default_factory=list)
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
    analysis_name: str | None = None
    custom_id: str | None = None
    corpus_id: UUID
    transcript_document_ids: list[UUID]
    cancel_requested: bool
    research_query: str | None = None
    researcher_topics: str | None = None

    codebook_id: UUID | None = None
    application_run_id: UUID | None = None
    documents_total: int
    documents_done: int
    analysis_units_total: int
    analysis_units_done: int
    passages_total: int
    passages_done: int
    transcripts_processed: int | None = None
    passages_processed: int | None = None
    quotes_created: int | None = None
    themes_created: int | None = None
    codes_created: int | None = None
    documents_coded: int | None = None
    documents_failed: int | None = None
    max_refinement_rounds: int
    apply_after_generation: bool
    error_message: str | None = None
    provenance_json: str | None = None
    action_log_json: str | None = None

    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    llm_tokens_input: int | None = None
    llm_tokens_output: int | None = None


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
