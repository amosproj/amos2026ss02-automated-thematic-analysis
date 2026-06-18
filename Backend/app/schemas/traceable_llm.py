from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


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


class CodebookReviewAction(BaseModel):
    action: ReviewActionType
    target: str | None = None
    replacement: str | None = None
    source_labels: list[str] = Field(default_factory=list)
    new_parent_path: list[str] = Field(default_factory=list)
    artifact_type: Literal["theme", "subtheme", "code"] | None = None
    reason: str | None = None


class CodebookReviewResult(BaseModel):
    actions: list[CodebookReviewAction] = Field(default_factory=list)


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
