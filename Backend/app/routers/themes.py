from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.dependencies import AppSettings, DbSession
from app.exceptions import NotFoundError, UnprocessableError
from app.schemas.common import Page, ResponseEnvelope
from app.schemas.theme_graph import ThemeTreeNode
from app.schemas.theme_views import (
    ThemeDemographicBreakdownResponse,
    ThemeFrequencyItem,
    ThemeQuoteItem,
)
from app.services.theme_demographic_breakdown import (
    MAX_BIN_COUNT,
    MIN_BIN_COUNT,
    ThemeDemographicBreakdownService,
)
from app.services.theme_frequency import ThemeFrequencyService
from app.services.theme_graph import ThemeGraphService, ThemeNotFoundError, ThemeValidationError
from app.services.theme_quotes import ThemeQuotesService

router = APIRouter(prefix="/codebooks/{codebook_id}/themes", tags=["themes"])


@router.get("", response_model=ResponseEnvelope[list[ThemeFrequencyItem]])
async def list_themes_with_frequency(
    codebook_id: UUID,
    session: DbSession,
    application_run_id: UUID | None = Query(
        default=None,
        description="Optional application run used to compute theme coverage. Defaults to the latest successful run.",
    ),
) -> JSONResponse:
    service = ThemeFrequencyService(session)
    try:
        payload = await service.list_theme_frequencies(
            codebook_id=codebook_id,
            application_run_id=application_run_id,
        )
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


@router.get(
    "/{theme_id}/demographic-breakdown",
    response_model=ResponseEnvelope[ThemeDemographicBreakdownResponse],
    summary="Break a theme's frequency down by demographic groups",
    description=(
        "For each requested demographic dimension, return the theme's frequency "
        "split by group (absolute count and percentage within the group). "
        "Unknown dimensions are ignored; an empty/zero result is returned when the "
        "codebook has no analysis run."
    ),
)
async def get_theme_demographic_breakdown(
    codebook_id: UUID,
    theme_id: UUID,
    session: DbSession,
    settings: AppSettings,
    dimensions: str = Query(
        default="",
        description="Comma-separated demographic dimension names to break down by.",
    ),
    application_run_id: UUID | None = Query(default=None),
    bins: str = Query(
        default="",
        description=(
            "Comma-separated dimension:bin_count pairs (e.g. 'age:5'), for "
            "grouping a numeric dimension into equal-width intervals instead "
            "of one group per raw value. Only meaningful for numeric "
            "dimensions; ignored for dimensions not also passed in `dimensions`."
        ),
    ),
) -> JSONResponse:
    requested = [part.strip() for part in dimensions.split(",") if part.strip()]
    requested_bins = _parse_bins_param(bins)
    service = ThemeDemographicBreakdownService(
        session,
        small_sample_threshold=settings.DEMOGRAPHIC_SMALL_SAMPLE_THRESHOLD,
    )
    try:
        payload = await service.get_theme_breakdown(
            codebook_id=codebook_id,
            theme_id=theme_id,
            dimensions=requested,
            application_run_id=application_run_id,
            bins=requested_bins,
        )
    except ThemeNotFoundError as exc:
        raise NotFoundError(str(exc)) from exc

    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))


def _parse_bins_param(raw: str) -> dict[str, int]:
    bins: dict[str, int] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        name, _, count_text = part.partition(":")
        name = name.strip()
        count_text = count_text.strip()
        if not name or not count_text:
            continue
        try:
            count = int(count_text)
        except ValueError as exc:
            raise UnprocessableError(
                f"Invalid bin count for dimension '{name}': '{count_text}'"
            ) from exc
        if not (MIN_BIN_COUNT <= count <= MAX_BIN_COUNT):
            raise UnprocessableError(
                f"Bin count for dimension '{name}' must be between "
                f"{MIN_BIN_COUNT} and {MAX_BIN_COUNT}; got {count}."
            )
        bins[name] = count
    return bins


@router.get("/{theme_id}/quotes", response_model=ResponseEnvelope[Page[ThemeQuoteItem]])
async def list_theme_quotes(
    codebook_id: UUID,
    theme_id: UUID,
    session: DbSession,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    application_run_id: UUID | None = Query(default=None),
    include_descendants: bool = Query(default=True),
) -> JSONResponse:
    service = ThemeQuotesService(session)
    payload = await service.list_theme_quotes(
        codebook_id=codebook_id,
        theme_id=theme_id,
        page=page,
        page_size=page_size,
        application_run_id=application_run_id,
        include_descendants=include_descendants,
    )
    return JSONResponse(content=ResponseEnvelope.ok(payload).model_dump(mode="json"))
