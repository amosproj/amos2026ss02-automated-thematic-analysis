from pydantic import BaseModel, Field


class ThemePresence(BaseModel):
    theme_label: str = Field(description="The name or label of the theme from the codebook.")
    present: bool = Field(description="True if the theme is present in the transcript, False otherwise.")
    confidence: float = Field(description="A confidence score from 0.0 to 1.0 indicating how confident you are that the theme is present.")
    quote: str | None = Field(None, description="A verbatim illustrative quote from the transcript supporting the theme's presence. Required if present is True.")

class InterviewAnalysisResult(BaseModel):
    themes: list[ThemePresence] = Field(description="The list of themes evaluated against the transcript.")
    summary: str | None = Field(None, description="A 2-3 sentence orientation to what the interview is about.")
    researcher_notes: str | None = Field(None, description="Anything ambiguous, contradictory, or worth a follow-up interview question.")


class GeneratedThemeNode(BaseModel):
    label: str = Field(description="Theme node label, e.g., a theme or subtheme name.")
    description: str | None = Field(
        None,
        description="Optional short description for this node.",
    )


class GeneratedThemePath(BaseModel):
    path: list[GeneratedThemeNode] = Field(
        description="Ordered theme path from root theme to deepest subtheme.",
    )


class GeneratedCodeSuggestion(BaseModel):
    label: str = Field(description="Code label.")
    description: str | None = Field(None, description="Optional short code description.")
    theme_path: list[str] = Field(
        description="Theme path (root -> ... -> leaf theme) this code belongs to.",
    )


class PassageCodebookGeneration(BaseModel):
    themes: list[GeneratedThemePath] = Field(
        description="Theme/subtheme paths identified in one passage.",
    )
    codes: list[GeneratedCodeSuggestion] = Field(
        description="Codes identified in one passage.",
    )


class CodeConsolidationItem(BaseModel):
    label: str = Field(description="Code label.")
    description: str | None = Field(None, description="Optional short code description.")


class CodeConsolidationResult(BaseModel):
    codes: list[CodeConsolidationItem] = Field(
        description="Consolidated, non-overlapping codes for the generated codebook.",
    )
