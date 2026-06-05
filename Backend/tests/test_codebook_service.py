"""Unit tests for app/services/codebook.py (CodebookService).

Uses the shared in-memory SQLite fixtures from conftest.py.
"""
from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select

from app.exceptions import NotFoundError
from app.models.ingestion import Corpus
from app.schemas.codebook import CodebookCreateRequest, NodeInput
from app.services.codebook import CodebookService

CORPUS_ID = "00000000-0000-0000-0000-000000000001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _ensure_corpus(session, corpus_id: str = CORPUS_ID):
    uid = uuid.UUID(corpus_id)
    if not (await session.execute(select(Corpus.id).where(Corpus.id == uid))).scalar_one_or_none():
        session.add(Corpus(id=uid, project_id=uuid.uuid4(), name="Test Corpus"))
        await session.commit()


def _theme(n: int) -> NodeInput:
    return NodeInput(name=f"Theme {n}", description=f"Description for theme {n}")


def _payload(n_themes: int = 3, corpus_id: str = CORPUS_ID) -> CodebookCreateRequest:
    return CodebookCreateRequest(
        name="Test Codebook",
        corpus_id=corpus_id,
        nodes=[_theme(i) for i in range(1, n_themes + 1)],
    )


# ---------------------------------------------------------------------------
# create_codebook
# ---------------------------------------------------------------------------


async def test_create_codebook_with_one_theme(db_session):
    await _ensure_corpus(db_session)
    svc = CodebookService(db_session)
    codebook, themes, edges, _, _ = await svc.create_codebook(_payload(1))

    assert codebook.id is not None
    assert codebook.name == "Test Codebook"
    assert codebook.version == 1
    assert codebook.corpus_id == uuid.UUID(CORPUS_ID)
    assert codebook.created_by == "researcher"
    assert len(themes) == 1
    assert themes[0].label == "Theme 1"


async def test_create_codebook_with_fifty_themes(db_session):
    await _ensure_corpus(db_session)
    svc = CodebookService(db_session)
    codebook, themes, edges, _, _ = await svc.create_codebook(_payload(50))
    assert len(themes) == 50


async def test_create_codebook_auto_increments_version(db_session):
    await _ensure_corpus(db_session)
    svc = CodebookService(db_session)

    cb1, _, _, _, _ = await svc.create_codebook(_payload())
    cb2, _, _, _, _ = await svc.create_codebook(_payload())

    assert cb1.version == 1
    assert cb2.version == 2


async def test_create_codebook_versions_are_scoped_per_corpus(db_session):
    await _ensure_corpus(db_session, "00000000-0000-0000-0000-000000000002")
    await _ensure_corpus(db_session, "00000000-0000-0000-0000-000000000003")
    svc = CodebookService(db_session)

    cb_p1, _, _, _, _ = await svc.create_codebook(_payload(corpus_id="00000000-0000-0000-0000-000000000002"))
    cb_p2, _, _, _, _ = await svc.create_codebook(_payload(corpus_id="00000000-0000-0000-0000-000000000003"))

    # Both are first codebooks for their respective projects → both version 1.
    assert cb_p1.version == 1
    assert cb_p2.version == 1


async def test_create_codebook_persists_all_themes(db_session):
    await _ensure_corpus(db_session)
    svc = CodebookService(db_session)
    _, themes, _, _, _ = await svc.create_codebook(_payload(5))

    labels = {t.label for t in themes}
    assert labels == {"Theme 1", "Theme 2", "Theme 3", "Theme 4", "Theme 5"}


# ---------------------------------------------------------------------------
# get_codebook_detail
# ---------------------------------------------------------------------------


async def test_get_codebook_detail_returns_correct_themes(db_session):
    await _ensure_corpus(db_session)
    svc = CodebookService(db_session)
    created_cb, created_themes, _, _, _ = await svc.create_codebook(_payload(3))

    fetched_cb, fetched_themes, _, _, _ = await svc.get_codebook_detail(created_cb.id)

    assert fetched_cb.id == created_cb.id
    assert len(fetched_themes) == 3
    fetched_labels = {t.label for t in fetched_themes}
    created_labels = {t.label for t in created_themes}
    assert fetched_labels == created_labels


async def test_get_codebook_detail_not_found_raises(db_session):
    svc = CodebookService(db_session)
    with pytest.raises(NotFoundError):
        await svc.get_codebook_detail(uuid.uuid4())


# ---------------------------------------------------------------------------
# delete_codebook
# ---------------------------------------------------------------------------


async def test_delete_codebook_cascades_and_succeeds(db_session):
    await _ensure_corpus(db_session)
    svc = CodebookService(db_session)
    created_cb, _, _, _, _ = await svc.create_codebook(_payload(3))

    await svc.delete_codebook(created_cb.id)

    with pytest.raises(NotFoundError):
        await svc.get_codebook_detail(created_cb.id)


async def test_delete_codebook_not_found_raises(db_session):
    svc = CodebookService(db_session)
    with pytest.raises(NotFoundError):
        await svc.delete_codebook(uuid.uuid4())


# ---------------------------------------------------------------------------
# build_detail_schema
# ---------------------------------------------------------------------------


async def test_build_detail_schema_shapes_output(db_session):
    await _ensure_corpus(db_session)
    svc = CodebookService(db_session)
    codebook, themes, edges, _, _ = await svc.create_codebook(_payload(2))
    schema = CodebookService.build_detail_schema(codebook, themes, edges)

    assert schema.name == "Test Codebook"
    assert len(schema.themes) == 2
    assert all(t.name and t.id for t in schema.themes)
