from __future__ import annotations

from pydantic import Field

from app.schemas.common import BaseSchema


class LlmProviderOption(BaseSchema):
    """One selectable LLM provider as exposed to the UI."""

    id: str = Field(description="Canonical provider id, e.g. 'FAU' or 'ACADEMIC'.")
    label: str = Field(description="Human-readable provider name.")
    description: str = Field(description="Short, non-technical explanation for tooltips.")
    has_api_key: bool = Field(
        description="Whether the server has an API key configured for this provider."
    )


class LlmProviderState(BaseSchema):
    """Current active provider plus the available options and the env default."""

    active: str = Field(description="Canonical id of the currently active provider.")
    default: str = Field(description="Env-configured fallback provider id.")
    available: list[LlmProviderOption] = Field(
        default_factory=list, description="All selectable providers, in display order."
    )


class LlmProviderUpdateRequest(BaseSchema):
    """Payload for selecting the active LLM provider."""

    provider: str = Field(description="Canonical provider id to activate.")
