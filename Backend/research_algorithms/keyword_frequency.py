from __future__ import annotations

import re
from collections import Counter

from app.generation.contracts import (
    CodebookDraft,
    CodeDraft,
    GenerationAlgorithm,
    GenerationContext,
    GenerationInput,
    GenerationResult,
    ThemeDraft,
)

_WORD_RE = re.compile(r"[A-Za-z][A-Za-z'-]{2,}")
_STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "because",
    "before",
    "being",
    "between",
    "could",
    "during",
    "every",
    "from",
    "have",
    "into",
    "more",
    "most",
    "only",
    "other",
    "over",
    "should",
    "such",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "through",
    "under",
    "were",
    "when",
    "where",
    "which",
    "while",
    "with",
    "would",
}


class KeywordFrequencyGenerationAlgorithm:
    """Deterministic local algorithm that derives codes from frequent words."""

    algorithm_id = "keyword_frequency_example"
    algorithm_version = "0.1"
    requires_llm = False

    async def generate(
        self,
        generation_input: GenerationInput,
        context: GenerationContext,
    ) -> GenerationResult:
        await context.set_phase("keyword_frequency_extracting")
        await context.report_progress(0, len(generation_input.documents))

        counter: Counter[str] = Counter()
        document_hits: dict[str, set[str]] = {}
        for index, document in enumerate(generation_input.documents, start=1):
            await context.raise_if_cancelled()
            document_terms = set(_tokenize(document.content))
            counter.update(document_terms)
            for term in document_terms:
                document_hits.setdefault(term, set()).add(str(document.document_id))
            await context.report_progress(index, len(generation_input.documents))

        ranked_terms = _rank_terms(counter, generation_input=generation_input)
        if not ranked_terms:
            ranked_terms = ("general-observation",)

        root_theme_key = "keyword_patterns"
        child_theme_key = "frequent_terms"
        themes = (
            ThemeDraft(
                key=root_theme_key,
                label="Keyword Patterns",
                description="Themes inferred from repeated transcript vocabulary.",
            ),
            ThemeDraft(
                key=child_theme_key,
                label="Frequent Terms",
                description="Codes grouped by high-frequency non-stopword terms.",
                parent_theme_key=root_theme_key,
            ),
        )
        codes = tuple(
            CodeDraft(
                key=f"term_{term.replace('-', '_')}",
                label=term.replace("-", " ").title(),
                description=_code_description(term, document_hits=document_hits),
                theme_key=child_theme_key,
            )
            for term in ranked_terms
        )
        return GenerationResult(
            codebook=CodebookDraft(themes=themes, codes=codes),
            provenance={
                "method": "deterministic keyword document-frequency baseline",
                "documents_seen": len(generation_input.documents),
                "selected_terms": list(ranked_terms),
                "research_query_used": bool(generation_input.research_query),
                "researcher_topics_used": bool(generation_input.researcher_topics),
            },
            action_log=(
                {
                    "action": "extract_keyword_frequency_codebook",
                    "documents": len(generation_input.documents),
                    "codes": len(codes),
                },
            ),
            processed_unit_count=len(generation_input.documents),
            quote_count=0,
            token_usage={"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
        )


def _tokenize(text: str) -> list[str]:
    return [
        token.casefold().replace("'", "").strip("-")
        for token in _WORD_RE.findall(text)
        if token.casefold() not in _STOPWORDS
    ]


def _rank_terms(counter: Counter[str], *, generation_input: GenerationInput) -> tuple[str, ...]:
    focus_order: dict[str, int] = {}
    for term in _tokenize(
        " ".join(
            value
            for value in (
                generation_input.research_query,
                generation_input.researcher_topics,
            )
            if value
        )
    ):
        focus_order.setdefault(term, len(focus_order))
    non_focus_rank = len(focus_order)
    return tuple(
        term
        for term, _count in sorted(
            counter.items(),
            key=lambda item: (
                focus_order.get(item[0], non_focus_rank),
                -item[1],
                item[0],
            ),
        )[:5]
    )


def _code_description(term: str, *, document_hits: dict[str, set[str]]) -> str:
    document_count = len(document_hits.get(term, set()))
    return f"Transcript term found in {document_count} selected document(s)."


def create() -> GenerationAlgorithm:
    return KeywordFrequencyGenerationAlgorithm()
