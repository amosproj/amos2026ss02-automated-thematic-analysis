from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.config import Settings
from app.dependencies import AppSettings, DbSession
from app.llm import providers
from app.schemas.common import ResponseEnvelope
from app.schemas.settings import (
    LlmProviderOption,
    LlmProviderState,
    LlmProviderUpdateRequest,
)
from app.services.app_settings import get_active_provider, set_active_provider

router = APIRouter(prefix="/settings", tags=["settings"])


def _build_state(*, active: str, settings: Settings) -> LlmProviderState:
    return LlmProviderState(
        active=active,
        default=providers.resolve_default(settings),
        available=[
            LlmProviderOption(
                id=spec.id,
                label=spec.label,
                description=spec.description,
                has_api_key=providers.has_api_key(settings, spec.id),
            )
            for spec in providers.available_providers()
        ],
    )


@router.get(
    "/llm-provider",
    response_model=ResponseEnvelope[LlmProviderState],
    summary="Get the active LLM provider and available options",
)
async def get_llm_provider(session: DbSession, settings: AppSettings) -> JSONResponse:
    active = await get_active_provider(session, settings=settings)
    state = _build_state(active=active, settings=settings)
    return JSONResponse(content=ResponseEnvelope.ok(state).model_dump(mode="json"))


@router.put(
    "/llm-provider",
    response_model=ResponseEnvelope[LlmProviderState],
    summary="Set the active LLM provider",
)
async def update_llm_provider(
    payload: LlmProviderUpdateRequest,
    session: DbSession,
    settings: AppSettings,
) -> JSONResponse:
    # set_active_provider validates the id and key presence, raising
    # UnprocessableError (422) with a user-friendly message on failure.
    active = await set_active_provider(session, payload.provider, settings=settings)
    state = _build_state(active=active, settings=settings)
    return JSONResponse(content=ResponseEnvelope.ok(state).model_dump(mode="json"))
