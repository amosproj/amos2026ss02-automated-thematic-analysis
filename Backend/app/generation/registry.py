"""Administrator-controlled generation algorithm catalogue.

The UI selects stable IDs from this list. Import paths remain server-controlled
so a browser request cannot cause an arbitrary Python module to be imported.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.config import Settings


@dataclass(frozen=True)
class AlgorithmSpec:
    id: str
    label: str
    description: str
    module_spec: str
    supports_refinement: bool = False


ALGORITHMS: tuple[AlgorithmSpec, ...] = (
    AlgorithmSpec(
        id="traceable_analysis",
        label="Traceable Analysis",
        description="Quote-grounded AI generation with iterative review and refinement.",
        module_spec="app.generation.algorithms.traceable:create",
        supports_refinement=True,
    ),
    AlgorithmSpec(
        id="keyword_frequency_example",
        label="Keyword Frequency (Demo)",
        description="A simple local baseline using frequent transcript words. No LLM required.",
        module_spec="research_algorithms.keyword_frequency:create",
    ),
)


def available_algorithms(settings: Settings) -> tuple[AlgorithmSpec, ...]:
    configured = settings.GENERATION_ALGORITHM.strip()
    if any(spec.module_spec == configured for spec in ALGORITHMS):
        return ALGORITHMS
    return (
        *ALGORITHMS,
        AlgorithmSpec(
            id="configured_custom",
            label="Custom Algorithm",
            description="The local generation algorithm configured by the server administrator.",
            module_spec=configured,
        ),
    )


def default_algorithm(settings: Settings) -> AlgorithmSpec:
    return next(
        spec
        for spec in available_algorithms(settings)
        if spec.module_spec == settings.GENERATION_ALGORITHM.strip()
    )
