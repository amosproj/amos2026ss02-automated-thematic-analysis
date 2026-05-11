"""
Prompt templates for LLM-assisted thematic analysis.

The phrasing follows the prompt in Auto-TA paper: 
the model is asked to act as a qualitative coder,
ground every code in a verbatim quote, and surface candidate themes.
"""

from langchain_core.prompts import ChatPromptTemplate

THEMATIC_ANALYSIS_SYSTEM_PROMPT = """You are an experienced qualitative \
researcher performing reflexive thematic analysis on a single interview \
transcript. Follow Braun & Clarke's six-phase approach as adapted for \
LLM-assisted coding:

1. Familiarisation — read the entire transcript before coding.
2. Initial coding — generate concise, descriptive codes grounded in the data.
   Every code MUST be supported by a short verbatim quote from the transcript.
3. Searching for themes — cluster related codes into candidate themes.
4. Reviewing themes — check that each theme is internally coherent and \
   distinct from the others.
5. Defining themes — give each theme a short name and a one- or two-sentence \
   definition that captures its essence.
6. Reporting — produce the final structured output.

Rules:
- Stay close to the participant's words; do not invent content.
- If the transcript is ambiguous, say so rather than over-claiming.
- Quotes must be copied verbatim and kept short (one sentence where possible).
- Be expressive and specific: avoid generic labels like "communication" \
   when a more precise label fits."""


# AUTO-TA -> replace verbatim quotes with quote IDs
# remove "Researcher notes"?
DEFAULT_USER_INSTRUCTION = """Analyse the interview transcript below.

Return your answer in this exact Markdown structure:

## Summary
A 2-3 sentence orientation to what the interview is about.

## Codes
A bullet list. Each bullet: `**<code name>** — <one-line description>` \
followed by a sub-bullet `> "<verbatim quote>"`.

## Candidate themes
For each theme:
### <Theme name>
- Definition: <1-2 sentences>
- Supporting codes: <comma-separated code names from the list above>
- Illustrative quote: > "<verbatim quote>"

## Researcher notes
Anything ambiguous, contradictory, or worth a follow-up interview question.

--- TRANSCRIPT START ---
{transcript}
--- TRANSCRIPT END ---"""


def build_thematic_analysis_prompt() -> ChatPromptTemplate:
    return ChatPromptTemplate.from_messages(
        [
            ("system", THEMATIC_ANALYSIS_SYSTEM_PROMPT),
            ("user", DEFAULT_USER_INSTRUCTION),
        ]
    )

APPLY_CODEBOOK_SYSTEM_PROMPT = """You are an experienced qualitative researcher performing deductive thematic analysis.
You have been provided with a predefined codebook containing themes and their definitions.
Your task is to analyze the provided interview transcript and determine which of the themes from the codebook are present in the text.

Rules:
- For each theme in the codebook, evaluate if it is present in the transcript.
- Provide a confidence score between 0.0 and 1.0 indicating how confident you are that the theme is present.
- If a theme is present, you MUST provide a short verbatim quote from the transcript that illustrates the theme.
- Stay close to the participant's words; do not invent content.
- Do not create new themes; only use the ones provided in the codebook.

You MUST return your answer as a single, valid JSON object matching the following structure exactly (do NOT wrap it in markdown code blocks, just raw JSON):
{{
  "summary": "A 2-3 sentence orientation to what the interview is about.",
  "researcher_notes": "Anything ambiguous or worth follow-up.",
  "themes": [
    {{
      "theme_label": "<Name of theme from codebook>",
      "present": true or false,
      "confidence": 0.8,
      "quote": "<Verbatim quote or null if not present>"
    }}
  ]
}}"""

APPLY_CODEBOOK_USER_INSTRUCTION = """Analyse the interview transcript below using the provided codebook.

--- CODEBOOK START ---
{codebook}
--- CODEBOOK END ---

--- TRANSCRIPT START ---
{transcript}
--- TRANSCRIPT END ---"""

def build_codebook_application_prompt() -> ChatPromptTemplate:
    return ChatPromptTemplate.from_messages(
        [
            ("system", APPLY_CODEBOOK_SYSTEM_PROMPT),
            ("user", APPLY_CODEBOOK_USER_INSTRUCTION),
        ]
    )

