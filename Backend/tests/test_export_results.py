"""Tests for GET /codebook-application-runs/{run_id}/export.

Demographics/transcripts are seeded via the HTTP API (so real linking runs);
codebook/themes/runs/assignments are inserted directly (no LLM in tests).
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Codebook, CodebookApplicationRun, Theme, ThemeHierarchyRelationship
from app.models.analysis import DocumentCoding, ThemeAssignment

INGESTION_API = "/api/v1/ingestion"
DEMOGRAPHIC_API = "/api/v1/demographic"
EXPORT_API = "/api/v1/codebook-application-runs"


def _parse_csv(text: str) -> tuple[list[str], list[list[str]]]:
    rows = list(csv.reader(io.StringIO(text)))
    return (rows[0], rows[1:]) if rows else ([], [])


# ── seed helpers ──────────────────────────────────────────────────────────────

@dataclass
class _Seed:
    run_id: UUID
    linked_participant_id: str  # "P-101" title matches a demographic username
    unlinked_doc_title: str  # "Unlinked-Doc" no match; doc title is the fallback


async def _seed_full(client, db_engine) -> _Seed:
    """Rich dataset built via the real HTTP API so the linking service (not a
    manual FK patch) sets CorpusDocument.demographic_row_id."""
    corpus_id = uuid4()
    resp = await client.post(
        f"{INGESTION_API}/corpora",
        json={"corpus_id": str(corpus_id), "name": "Export Test Corpus"},
    )
    assert resp.status_code == 201, resp.text

    # "P-101" matches a demographic username → auto-linked; "Unlinked-Doc" stays unlinked.
    resp = await client.post(
        f"{INGESTION_API}/corpora/{corpus_id}/documents/bulk",
        json={"documents": [
            {"title": "P-101", "text": "Interview text for participant P-101."},
            {"title": "Unlinked-Doc", "text": "Interview text for unlinked participant."},
        ]},
    )
    assert resp.status_code == 201

    # username is the link key, excluded from the export's demographic columns.
    csv_content = "username;age;role\nP-101;30;Engineer\n"
    upload = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/upload",
        files={"file": ("demo.csv", csv_content, "application/octet-stream")},
    )
    assert upload.status_code == 201
    import_id = upload.json()["data"]["import_id"]

    # Confirm triggers auto-linking by title/username match.
    confirm = await client.post(
        f"{DEMOGRAPHIC_API}/{corpus_id}/confirm",
        params={"import_id": import_id, "confirm": True},
    )
    assert confirm.status_code == 201

    summary = (await client.get(f"{DEMOGRAPHIC_API}/{corpus_id}/link-summary")).json()["data"]
    doc_by_title = {d["document_title"]: UUID(d["document_id"]) for d in summary["details"]}
    linked_doc_id = doc_by_title["P-101"]
    unlinked_doc_id = doc_by_title["Unlinked-Doc"]

    # Codebook/themes/run/assignments seeded directly (no LLM in tests).
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        codebook_id = uuid4()
        run_id = uuid4()
        parent_id = uuid4()
        theme1_id = uuid4()
        theme2_id = uuid4()
        linked_coding_id = uuid4()
        unlinked_coding_id = uuid4()

        session.add_all([
            Codebook(id=codebook_id, corpus_id=corpus_id, name="Export Codebook",
                     description="", version=1, created_by="system"),
            Theme(id=parent_id, codebook_id=codebook_id, label="Parent A", is_active=True),
            Theme(id=theme1_id, codebook_id=codebook_id, label="Theme 1", is_active=True),
            Theme(id=theme2_id, codebook_id=codebook_id, label="Theme 2", is_active=True),
            ThemeHierarchyRelationship(id=uuid4(), codebook_id=codebook_id,
                                       parent_theme_id=parent_id, child_theme_id=theme1_id,
                                       is_active=True),
            CodebookApplicationRun(id=run_id, corpus_id=corpus_id, codebook_id=codebook_id,
                                   status="succeeded"),
            DocumentCoding(id=linked_coding_id, application_run_id=run_id,
                           document_id=linked_doc_id, codebook_id=codebook_id),
            DocumentCoding(id=unlinked_coding_id, application_run_id=run_id,
                           document_id=unlinked_doc_id, codebook_id=codebook_id),
        ])
        await session.flush()

        session.add_all([
            # P-101 gets two quotes on Theme 1 and one on Theme 2.
            ThemeAssignment(id=uuid4(), document_coding_id=linked_coding_id,
                            theme_id=theme1_id, is_present=True, quote="Quote 1a"),
            ThemeAssignment(id=uuid4(), document_coding_id=linked_coding_id,
                            theme_id=theme1_id, is_present=True, quote="Quote 1b"),
            ThemeAssignment(id=uuid4(), document_coding_id=linked_coding_id,
                            theme_id=theme2_id, is_present=True, quote="Quote 2"),
            # Unlinked transcript gets one quote on Theme 1.
            ThemeAssignment(id=uuid4(), document_coding_id=unlinked_coding_id,
                            theme_id=theme1_id, is_present=True, quote="Unlinked quote"),
            # Excluded: null quote and absent assignment.
            ThemeAssignment(id=uuid4(), document_coding_id=linked_coding_id,
                            theme_id=theme1_id, is_present=True, quote=None),
            ThemeAssignment(id=uuid4(), document_coding_id=linked_coding_id,
                            theme_id=theme1_id, is_present=False, quote="Absent"),
        ])
        await session.commit()

    return _Seed(run_id=run_id, linked_participant_id="P-101", unlinked_doc_title="Unlinked-Doc")


async def _seed_empty(client, db_engine) -> UUID:
    """Run with no assignments and no demographic file."""
    corpus_id = uuid4()
    resp = await client.post(
        f"{INGESTION_API}/corpora",
        json={"corpus_id": str(corpus_id), "name": "Empty Export Corpus"},
    )
    assert resp.status_code == 201, resp.text

    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        codebook_id = uuid4()
        run_id = uuid4()
        session.add_all([
            Codebook(id=codebook_id, corpus_id=corpus_id, name="Empty Codebook",
                     description="", version=1, created_by="system"),
            CodebookApplicationRun(id=run_id, corpus_id=corpus_id, codebook_id=codebook_id,
                                   status="succeeded"),
        ])
        await session.commit()
    return run_id


async def _seed_multi_theme_quote(client, db_engine) -> UUID:
    """One participant has a single quote tagged with two themes, plus another
    quote on one of those themes. A second participant has one quote. No
    demographic file, so demo_columns is empty for this run.
    """
    corpus_id = uuid4()
    resp = await client.post(
        f"{INGESTION_API}/corpora",
        json={"corpus_id": str(corpus_id), "name": "Multi Theme Quote Corpus"},
    )
    assert resp.status_code == 201, resp.text

    resp = await client.post(
        f"{INGESTION_API}/corpora/{corpus_id}/documents/bulk",
        json={"documents": [
            {"title": "P-Multi", "text": "Transcript for P-Multi."},
            {"title": "P-Other", "text": "Transcript for P-Other."},
        ]},
    )
    assert resp.status_code == 201

    docs = (await client.get(f"{INGESTION_API}/corpora/{corpus_id}/documents")).json()["data"]["items"]
    doc_by_title = {d["title"]: UUID(d["id"]) for d in docs}

    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        codebook_id = uuid4()
        run_id = uuid4()
        alpha_id = uuid4()
        beta_id = uuid4()
        multi_coding_id = uuid4()
        other_coding_id = uuid4()

        session.add_all([
            Codebook(id=codebook_id, corpus_id=corpus_id, name="Multi Theme Codebook",
                     description="", version=1, created_by="system"),
            Theme(id=alpha_id, codebook_id=codebook_id, label="Alpha", is_active=True),
            Theme(id=beta_id, codebook_id=codebook_id, label="Beta", is_active=True),
            CodebookApplicationRun(id=run_id, corpus_id=corpus_id, codebook_id=codebook_id,
                                   status="succeeded"),
            DocumentCoding(id=multi_coding_id, application_run_id=run_id,
                           document_id=doc_by_title["P-Multi"], codebook_id=codebook_id),
            DocumentCoding(id=other_coding_id, application_run_id=run_id,
                           document_id=doc_by_title["P-Other"], codebook_id=codebook_id),
        ])
        await session.flush()

        session.add_all([
            # "Zebra insight" is tagged on both themes these two rows must
            # stay adjacent in the export, not be split apart by theme grouping.
            ThemeAssignment(id=uuid4(), document_coding_id=multi_coding_id,
                            theme_id=beta_id, is_present=True, quote="Zebra insight"),
            ThemeAssignment(id=uuid4(), document_coding_id=multi_coding_id,
                            theme_id=alpha_id, is_present=True, quote="Zebra insight"),
            ThemeAssignment(id=uuid4(), document_coding_id=multi_coding_id,
                            theme_id=alpha_id, is_present=True, quote="Apple insight"),
            ThemeAssignment(id=uuid4(), document_coding_id=other_coding_id,
                            theme_id=alpha_id, is_present=True, quote="Other insight"),
        ])
        await session.commit()
    return run_id


# ── 404 ──────────────────────────────────────────────────────────────────────

async def test_unknown_run_returns_404(client) -> None:
    resp = await client.get(f"{EXPORT_API}/{uuid4()}/export", params={"format": "theme-based"})
    assert resp.status_code == 404


# ── response shape ────────────────────────────────────────────────────────────

async def test_response_is_csv_with_content_disposition(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export", params={"format": "theme-based"})
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert str(seed.run_id) in resp.headers["content-disposition"]
    assert "theme-based" in resp.headers["content-disposition"]


# ── theme-based ───────────────────────────────────────────────────────────────

async def test_theme_based_headers(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export", params={"format": "theme-based"})
    headers, _ = _parse_csv(resp.text)
    assert headers == ["Theme Name", "Parent Theme", "Theme Description", "Participant ID", "Quote"]


async def test_theme_based_row_count_excludes_null_and_absent(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export", params={"format": "theme-based"})
    _, rows = _parse_csv(resp.text)
    # 2×Theme1(P-101) + 1×Theme2(P-101) + 1×Theme1(unlinked) = 4
    assert len(rows) == 4


async def test_theme_based_parent_theme_populated_for_child(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export", params={"format": "theme-based"})
    _, rows = _parse_csv(resp.text)
    theme1_rows = [r for r in rows if r[0] == "Theme 1"]
    assert all(r[1] == "Parent A" for r in theme1_rows)


async def test_theme_based_root_theme_has_empty_parent(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export", params={"format": "theme-based"})
    _, rows = _parse_csv(resp.text)
    theme2_rows = [r for r in rows if r[0] == "Theme 2"]
    assert all(r[1] == "" for r in theme2_rows)


async def test_theme_based_multi_quote_same_participant_and_theme(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export", params={"format": "theme-based"})
    _, rows = _parse_csv(resp.text)
    p101_theme1_quotes = {r[4] for r in rows
                          if r[0] == "Theme 1" and r[3] == seed.linked_participant_id}
    assert p101_theme1_quotes == {"Quote 1a", "Quote 1b"}


async def test_theme_based_unlinked_transcript_uses_document_title(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export", params={"format": "theme-based"})
    _, rows = _parse_csv(resp.text)
    assert seed.unlinked_doc_title in {r[3] for r in rows}


# ── participant-based ─────────────────────────────────────────────────────────

async def test_participant_based_headers_include_demo_columns(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export",
                            params={"format": "participant-based"})
    headers, _ = _parse_csv(resp.text)
    assert headers == ["Participant ID", "age", "role", "Theme Name", "Quote"]


async def test_participant_based_row_count(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export",
                            params={"format": "participant-based"})
    _, rows = _parse_csv(resp.text)
    assert len(rows) == 4


async def test_participant_based_demographics_repeated_per_quote(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export",
                            params={"format": "participant-based"})
    _, rows = _parse_csv(resp.text)
    p101_rows = [r for r in rows if r[0] == seed.linked_participant_id]
    assert len(p101_rows) == 3  # Quote 1a, Quote 1b, Quote 2
    assert all(r[1] == "30" and r[2] == "Engineer" for r in p101_rows)


async def test_participant_based_unlinked_transcript_has_blank_demo_cells(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export",
                            params={"format": "participant-based"})
    _, rows = _parse_csv(resp.text)
    unlinked = [r for r in rows if r[0] == seed.unlinked_doc_title]
    assert len(unlinked) == 1
    assert unlinked[0][1] == "" and unlinked[0][2] == ""  # blank age, blank role


async def test_participant_based_rows_grouped_by_participant(client, db_engine) -> None:
    seed = await _seed_full(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{seed.run_id}/export",
                            params={"format": "participant-based"})
    _, rows = _parse_csv(resp.text)
    # to_participant_based_csv() sorts by participant, so a participant's
    # rows arrive contiguously.
    participant_col = [r[0] for r in rows]
    assert participant_col == sorted(participant_col)


async def test_participant_based_full_sort_order(client, db_engine) -> None:
    """Sorted by participant, then quote, then theme so the two rows of a
    quote tagged with multiple themes stay adjacent."""
    run_id = await _seed_multi_theme_quote(client, db_engine)
    resp = await client.get(f"{EXPORT_API}/{run_id}/export", params={"format": "participant-based"})
    _, rows = _parse_csv(resp.text)

    # [Participant ID, Theme Name, Quote] no demographic columns for this corpus.
    assert rows == [
        ["P-Multi", "Alpha", "Apple insight"],
        ["P-Multi", "Alpha", "Zebra insight"],
        ["P-Multi", "Beta", "Zebra insight"],
        ["P-Other", "Alpha", "Other insight"],
    ]


# ── edge cases ────────────────────────────────────────────────────────────────

async def test_empty_run_returns_header_only_csv(client, db_engine) -> None:
    run_id = await _seed_empty(client, db_engine)
    for fmt in ("theme-based", "participant-based"):
        resp = await client.get(f"{EXPORT_API}/{run_id}/export", params={"format": fmt})
        assert resp.status_code == 200
        _, rows = _parse_csv(resp.text)
        assert rows == [], f"expected no data rows for format={fmt}"
