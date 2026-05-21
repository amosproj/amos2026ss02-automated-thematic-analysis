from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from starlette.datastructures import UploadFile

from app.exceptions import UnprocessableError
from app.routers import demographic as demographic_router
from app.schemas.demographic import ImportDemographicPreview, ImportDemographicResponse


@pytest.mark.asyncio
async def test_confirm_demographic_upload_returns_fail_envelope_on_unprocessable():
    with patch.object(
        demographic_router.DemographicService,
        "confirm_demographic_upload",
        new=AsyncMock(side_effect=UnprocessableError("boom")),
    ):
        response = await demographic_router.confirm_demographic_upload(
            corpus_id=uuid4(),
            import_id=uuid4(),
            confirm=True,
            settings=SimpleNamespace(MAX_UPLOAD_BYTES=10),
            session=AsyncMock(),
        )
    assert response.status_code == 422
    payload = json.loads(response.body)
    assert payload["success"] is False
    assert payload["error"] == "UnprocessableError"


@pytest.mark.asyncio
async def test_list_files_returns_fail_envelope_on_unprocessable():
    with patch.object(
        demographic_router.DemographicService,
        "list_files",
        new=AsyncMock(side_effect=UnprocessableError("boom")),
    ):
        response = await demographic_router.list_demographic_files(
            corpus_id=uuid4(),
            settings=SimpleNamespace(MAX_UPLOAD_BYTES=10),
            session=AsyncMock(),
            page=1,
            page_size=20,
        )
    assert response.status_code == 422
    payload = json.loads(response.body)
    assert payload["success"] is False
    assert payload["error"] == "UnprocessableError"


@pytest.mark.asyncio
async def test_list_rows_returns_fail_envelope_on_unprocessable():
    with patch.object(
        demographic_router.DemographicService,
        "list_rows",
        new=AsyncMock(side_effect=UnprocessableError("boom")),
    ):
        response = await demographic_router.list_demographic_rows(
            corpus_id=uuid4(),
            settings=SimpleNamespace(MAX_UPLOAD_BYTES=10),
            session=AsyncMock(),
            demographic_file_id=None,
            page=1,
            page_size=20,
        )
    assert response.status_code == 422
    payload = json.loads(response.body)
    assert payload["success"] is False
    assert payload["error"] == "UnprocessableError"


@pytest.mark.asyncio
async def test_upload_demographic_data_success_path():
    file = UploadFile(filename="x.csv", file=None)
    expected = ImportDemographicResponse(
        import_id=uuid4(),
        name="n",
        status="pending",
        preview=ImportDemographicPreview(rows_detected=1, columns_detected=2, sample_rows=[]),
        expires_at=__import__("datetime").datetime.now(__import__("datetime").UTC),
    )
    with patch.object(
        demographic_router.DemographicService,
        "upload_demographic_data",
        new=AsyncMock(return_value=expected),
    ):
        response = await demographic_router.upload_demographic_data(
            corpus_id=uuid4(),
            file=file,
            settings=SimpleNamespace(MAX_UPLOAD_BYTES=10),
            session=AsyncMock(),
            name=None,
        )
    assert response.success is True
