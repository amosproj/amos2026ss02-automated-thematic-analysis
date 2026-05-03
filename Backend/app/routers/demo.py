from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, select

from app.config import get_settings
from app.dependencies import DbSession
from app.models import Codebook

router = APIRouter(prefix="/demo", tags=["demo"])

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _serialize_codebook(codebook: Codebook) -> dict[str, Any]:
    return {
        "id": str(codebook.id),
        "project_id": codebook.project_id,
        "name": codebook.name,
        "version": codebook.version,
    }


async def _load_codebooks(session: DbSession) -> list[Codebook]:
    stmt = select(Codebook).order_by(Codebook.project_id.asc(), desc(Codebook.version))
    return list((await session.scalars(stmt)).all())


@router.get("/", response_class=HTMLResponse)
async def codebook_selection_screen(
    request: Request,
    session: DbSession,
) -> HTMLResponse:
    codebooks = await _load_codebooks(session)
    serialized_codebooks = [_serialize_codebook(codebook) for codebook in codebooks]
    return templates.TemplateResponse(
        request=request,
        name="demo/codebook_selection.html",
        context={
            "api_prefix": get_settings().API_V1_PREFIX,
            "codebooks": serialized_codebooks,
        },
    )


@router.get("/overview", response_class=HTMLResponse)
async def theme_overview_screen(
    request: Request,
    session: DbSession,
    project_id: str = Query(..., min_length=1),
    version: int | None = Query(default=None, ge=1),
) -> HTMLResponse:
    stmt = (
        select(Codebook)
        .where(Codebook.project_id == project_id)
        .order_by(desc(Codebook.version))
    )
    project_codebooks = list((await session.scalars(stmt)).all())
    if not project_codebooks:
        raise HTTPException(
            status_code=404,
            detail=f"No codebooks found for project '{project_id}'.",
        )

    available_versions = {codebook.version for codebook in project_codebooks}
    selected_version = version if version is not None else project_codebooks[0].version
    if selected_version not in available_versions:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Codebook version {selected_version} was not found "
                f"for project '{project_id}'."
            ),
        )

    selected_codebook = next(
        codebook for codebook in project_codebooks if codebook.version == selected_version
    )
    return templates.TemplateResponse(
        request=request,
        name="demo/theme_overview.html",
        context={
            "api_prefix": get_settings().API_V1_PREFIX,
            "project_id": project_id,
            "selected_version": selected_version,
            "selected_codebook_name": selected_codebook.name,
            "analysis_runs": [
                {"version": codebook.version, "name": codebook.name}
                for codebook in project_codebooks
            ],
        },
    )
