from __future__ import annotations

"""Theme read endpoints with project-aware codebook version resolution."""

from uuid import UUID

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.dependencies import DbSession
from app.exceptions import NotFoundError, UnprocessableError
from app.schemas.common import ResponseEnvelope
from app.schemas.theme_views import ThemeFrequencyResponse, ThemeTreeResponse
from app.services.theme_graph import ThemeNotFoundError, ThemeValidationError
from app.services.theme_read import CodebookResolutionError, ThemeReadService

router = APIRouter(prefix="/projects/{project_id}/themes", tags=["themes"])


@router.get("/tree", response_model=ResponseEnvelope[ThemeTreeResponse])
async def get_theme_tree(
    project_id: str,
    session: DbSession,
    version: int | None = Query(
        default=None,
        ge=1,
        description="Codebook version. If omitted, the latest version for the project is used.",
    ),
    root_theme_id: UUID | None = Query(
        default=None,
        description="Optional root theme id for subtree rendering.",
    ),
    include_candidate_nodes: bool = Query(
        default=True,
        description="Whether candidate themes should be included alongside active themes.",
    ),
) -> JSONResponse:
    service = ThemeReadService(session)
    try:
        payload = await service.get_theme_tree_for_project(
            project_id=project_id,
            version=version,
            root_theme_id=root_theme_id,
            include_candidate_nodes=include_candidate_nodes,
        )
    except CodebookResolutionError as exc:
        raise NotFoundError(str(exc)) from exc
    except ThemeNotFoundError as exc:
        raise NotFoundError(str(exc)) from exc
    except ThemeValidationError as exc:
        raise UnprocessableError(str(exc)) from exc
    except ValueError as exc:
        raise UnprocessableError(str(exc)) from exc

    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))


@router.get("/", response_model=ResponseEnvelope[ThemeFrequencyResponse])
async def get_theme_frequency_overview(
    project_id: str,
    session: DbSession,
    version: int | None = Query(
        default=None,
        ge=1,
        description="Codebook version. If omitted, the latest version for the project is used.",
    ),
    include_candidate_nodes: bool = Query(
        default=True,
        description="Whether candidate themes should be included alongside active themes.",
    ),
) -> JSONResponse:
    service = ThemeReadService(session)
    try:
        payload = await service.get_theme_frequency_for_project(
            project_id=project_id,
            version=version,
            include_candidate_nodes=include_candidate_nodes,
        )
    except CodebookResolutionError as exc:
        raise NotFoundError(str(exc)) from exc
    except ThemeNotFoundError as exc:
        raise NotFoundError(str(exc)) from exc
    except ThemeValidationError as exc:
        raise UnprocessableError(str(exc)) from exc
    except ValueError as exc:
        raise UnprocessableError(str(exc)) from exc

    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))
