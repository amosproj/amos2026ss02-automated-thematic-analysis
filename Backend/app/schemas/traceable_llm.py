from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class QuoteCodeSuggestion(BaseModel):
    quote: str = Field(description="Exact quote copied from the transcript.")
    code_label: str = Field(description="Concise grounded code label.")
    code_description: str | None = Field(None, description="Short scope definition for the code.")
    rationale: str | None = Field(None, description="Brief reason why the quote supports the code.")
    confidence: float = Field(default=0.8, description="Confidence score between 0.0 and 1.0.")


class QuoteCodeExtractionResult(BaseModel):
    quote_code_pairs: list[QuoteCodeSuggestion] = Field(
        description="Grounded quote-code pairs extracted from one transcript."
    )


CodeRelationship = Literal[
    "equivalent",
    "a_subordinate_to_b",
    "b_subordinate_to_a",
    "orthogonal",
]


class CodeRelationshipResult(BaseModel):
    relationship: CodeRelationship
    confidence: float = Field(default=0.0)
    reason: str | None = None


class BatchCodeRelationshipResult(CodeRelationshipResult):
    pair_id: int


class BatchCodeRelationshipResults(BaseModel):
    pairs: list[BatchCodeRelationshipResult] = Field(default_factory=list)


class SynthesizedThemeNode(BaseModel):
    label: str
    description: str | None = None


class SynthesizedThemePath(BaseModel):
    path: list[SynthesizedThemeNode]


class SynthesizedSubtheme(BaseModel):
    subtheme_label: str
    subtheme_description: str | None = None
    code_labels: list[str]


class SubthemeSynthesisResult(BaseModel):
    subthemes: list[SynthesizedSubtheme]


class SynthesizedTheme(BaseModel):
    theme_label: str
    theme_description: str | None = None
    subtheme_labels: list[str]


class ThemeSynthesisResult(BaseModel):
    themes: list[SynthesizedTheme]


class SynthesizedCode(BaseModel):
    code_label: str
    code_description: str | None = None
    theme_path: list[str]


class CodebookSynthesisResult(BaseModel):
    themes: list[SynthesizedThemePath]
    codes: list[SynthesizedCode]


ReviewActionType = Literal["generate", "merge", "split", "revise", "move", "delete"]


class CodebookSplitChild(BaseModel):
    code_label: str
    code_description: str | None = None
    source_quote_ids: list[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def accept_llm_aliases(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if "code_label" not in normalized and "label" in normalized:
            normalized["code_label"] = normalized["label"]
        if "code_description" not in normalized and "description" in normalized:
            normalized["code_description"] = normalized["description"]
        if "source_quote_ids" not in normalized:
            for alias in ("quote_ids", "source_quotes", "evidence_quote_ids"):
                if alias in normalized:
                    normalized["source_quote_ids"] = normalized[alias]
                    break
        return normalized


class CodebookReviewAction(BaseModel):
    action: ReviewActionType
    target: str | None = None
    replacement: str | None = None
    source_labels: list[str] = Field(default_factory=list)
    new_parent_path: list[str] = Field(default_factory=list)
    split_children: list[CodebookSplitChild] = Field(default_factory=list)
    artifact_type: Literal["theme", "subtheme", "code"] | None = None
    reason: str | None = None


class CodebookReviewResult(BaseModel):
    actions: list[CodebookReviewAction] = Field(default_factory=list)


class MissingCodeSuggestion(BaseModel):
    code_label: str
    code_description: str | None = None
    source_quote_ids: list[str] = Field(default_factory=list)
    reason: str | None = None


class MissingCodeGenerationResult(BaseModel):
    codes: list[MissingCodeSuggestion] = Field(default_factory=list)


class CodebookMissingConcept(BaseModel):
    label: str
    description: str | None = None
    evidence_quotes: list[str] = Field(default_factory=list)


class CodebookOverbroadCode(BaseModel):
    code_label: str
    reason: str | None = None
    suggested_split_labels: list[str] = Field(default_factory=list)


class CodebookQualityEvaluationResult(BaseModel):
    fitness_score: float = 0.75
    coverage_score: float = 0.75
    missing_concepts: list[CodebookMissingConcept] = Field(default_factory=list)
    overbroad_codes: list[CodebookOverbroadCode] = Field(default_factory=list)
    notes: str | None = None


class TraceableAppliedCodeAssignment(BaseModel):
    code_label: str
    theme_label: str | None = None
    quote: str
    confidence: float = 0.8
    rationale: str | None = None


class TraceableApplicationResult(BaseModel):
    summary: str | None = None
    researcher_notes: str | None = None
    codes: list[TraceableAppliedCodeAssignment]
