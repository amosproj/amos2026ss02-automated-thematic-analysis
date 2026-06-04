from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from app.exceptions import UnprocessableError
from app.services.demographic import DemographicService


def _settings(tmp_path):
    return SimpleNamespace(
        UPLOADS_DIR=str(tmp_path / "uploads"),
        DEMOGRAPHIC_UPLOAD_TTL_SECONDS=3600,
    )


@pytest.mark.asyncio
async def test_parse_demographic_csv_rejects_invalid_utf8(tmp_path):
    service = DemographicService(session=AsyncMock(), settings=_settings(tmp_path))
    with pytest.raises(UnprocessableError, match="Could not decode"):
        service._parse_demographic_csv("bad.csv", b"\xff\xfe bad")


@pytest.mark.asyncio
async def test_parse_demographic_csv_requires_username_column(tmp_path):
    service = DemographicService(session=AsyncMock(), settings=_settings(tmp_path))
    with pytest.raises(UnprocessableError, match="must include 'username'"):
        service._parse_demographic_csv("bad.csv", b"age;group\n20;a\n")


@pytest.mark.asyncio
async def test_parse_demographic_csv_rejects_empty_username(tmp_path):
    service = DemographicService(session=AsyncMock(), settings=_settings(tmp_path))
    with pytest.raises(UnprocessableError, match="invalid username"):
        service._parse_demographic_csv("bad.csv", b"username;group\n;a\n")


@pytest.mark.asyncio
async def test_validate_interviewee_ids_unique_rejects_duplicates_in_upload(tmp_path):
    session = AsyncMock()
    service = DemographicService(session=session, settings=_settings(tmp_path))
    parsed = service._parse_demographic_csv("x.csv", b"username;group\nu1;a\nu1;b\n")
    with pytest.raises(UnprocessableError, match="duplicate username"):
        await service._validate_interviewee_ids_unique(uuid.uuid4(), parsed.parsed_rows)


@pytest.mark.asyncio
async def test_validate_interviewee_ids_unique_rejects_existing_in_same_corpus(tmp_path):
    existing_result = Mock()
    existing_result.scalars.return_value = ["u1"]
    session = AsyncMock()
    session.execute = AsyncMock(return_value=existing_result)
    service = DemographicService(session=session, settings=_settings(tmp_path))
    parsed = service._parse_demographic_csv("x.csv", b"username;group\nu1;a\n")
    with pytest.raises(UnprocessableError, match="username already exists"):
        await service._validate_interviewee_ids_unique(uuid.uuid4(), parsed.parsed_rows)


@pytest.mark.asyncio
async def test_list_rows_rejects_file_not_in_corpus(tmp_path):
    none_result = Mock()
    none_result.scalar_one_or_none.return_value = None
    session = AsyncMock()
    session.execute = AsyncMock(return_value=none_result)
    service = DemographicService(session=session, settings=_settings(tmp_path))
    service._validate_corpus = AsyncMock(return_value=None)

    with pytest.raises(UnprocessableError, match="does not belong to corpus"):
        await service.list_rows(
            corpus_id=uuid.uuid4(),
            demographic_file_id=uuid.uuid4(),
            page=1,
            page_size=10,
        )


@pytest.mark.asyncio
async def test_delete_file_not_found(tmp_path):
    none_result = Mock()
    none_result.scalar_one_or_none.return_value = None
    session = AsyncMock()
    session.execute = AsyncMock(return_value=none_result)
    service = DemographicService(session=session, settings=_settings(tmp_path))
    service._validate_corpus = AsyncMock(return_value=None)
    
    from app.exceptions import NotFoundError
    with pytest.raises(NotFoundError, match="not found in corpus"):
        await service.delete_file(uuid.uuid4(), uuid.uuid4())

@pytest.mark.asyncio
async def test_delete_file_success(tmp_path):
    file_obj = Mock()
    result = Mock()
    result.scalar_one_or_none.return_value = file_obj
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result)
    service = DemographicService(session=session, settings=_settings(tmp_path))
    service._validate_corpus = AsyncMock(return_value=None)
    
    await service.delete_file(uuid.uuid4(), uuid.uuid4())
    session.delete.assert_called_once_with(file_obj)
    session.commit.assert_called_once()

