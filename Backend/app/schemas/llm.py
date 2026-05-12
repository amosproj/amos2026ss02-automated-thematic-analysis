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
