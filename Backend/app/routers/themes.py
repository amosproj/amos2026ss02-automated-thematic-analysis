from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.dependencies import DbSession
from app.exceptions import NotFoundError, UnprocessableError
from app.schemas.common import ResponseEnvelope
from app.schemas.theme_graph import ThemeTreeNode
from app.schemas.theme_views import ThemeFrequencyItem
from app.services.theme_frequency import ThemeFrequencyService
from app.services.theme_graph import ThemeGraphService, ThemeNotFoundError, ThemeValidationError

router = APIRouter(prefix="/codebooks/{codebook_id}/themes", tags=["themes"])


@router.get("", response_model=ResponseEnvelope[list[ThemeFrequencyItem]])
async def list_themes_with_frequency(
    codebook_id: UUID,
    session: DbSession,
) -> JSONResponse:
    service = ThemeFrequencyService(session)
    try:
        payload = await service.list_theme_frequencies(codebook_id=codebook_id)
    except ThemeNotFoundError as exc:
        raise NotFoundError(str(exc)) from exc

    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))


@router.get("/tree", response_model=ResponseEnvelope[list[ThemeTreeNode]])
async def get_theme_tree(
    codebook_id: UUID,
    session: DbSession,
    root_theme_id: UUID | None = Query(
        default=None,
        description="Optional root theme id to return only one subtree.",
    ),
) -> JSONResponse:
    # Delegate all hierarchy building + validation to the service layer
    service = ThemeGraphService(session)
    try:
        payload = await service.get_theme_tree(
            codebook_id=codebook_id,
            root_theme_id=root_theme_id,
        )
    except ThemeNotFoundError as exc:
        # Normalize domain-level "not found" cases to API-level 404 responses.
        raise NotFoundError(str(exc)) from exc
    except ThemeValidationError as exc:
        # Invalid hierarchy state (e.g., cycle/multi-parent) should surface as 422.
        raise UnprocessableError(str(exc)) from exc
    except ValueError as exc:
        # Guardrail for malformed/invalid value paths
        raise UnprocessableError(str(exc)) from exc

    # Wrap successful payloads in response envelope
    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))
