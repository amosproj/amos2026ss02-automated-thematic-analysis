from __future__ import annotations

import uuid
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import (
    Codebook,
    CodebookApplicationRun,
    CodebookThemeRelationship,
    CorpusDocument,
    DemographicFiles,
    DemographicRow,
    DocumentCoding,
    Theme,
    ThemeAssignment,
)


async def _seed(db_engine):
    """Seed one corpus with a gender dimension and a theme present for some men."""
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        corpus_id = uuid4()
        codebook_id = uuid4()
        theme_id = uuid4()
        run_id = uuid4()
        file_id = uuid4()

        session.add(
            Codebook(
                id=codebook_id,
                corpus_id=corpus_id,
                name="Breakdown API",
                description="Fixture",
                version=1,
                created_by="system",
            )
        )
        session.add(Theme(id=theme_id, codebook_id=codebook_id, label="Theme A", is_active=True))
        await session.flush()
        session.add(
            CodebookThemeRelationship(
                id=uuid4(), codebook_id=codebook_id, theme_id=theme_id, is_active=True
            )
        )
        session.add(
            DemographicFiles(
                id=file_id,
                name="people.csv",
                corpus_id=corpus_id,
                original_columns=["username", "gender"],
            )
        )

        people = {
            "m1": "male",
            "m2": "male",
            "f1": "female",
            "f2": "female",
        }
        present = {"m1", "f1", "f2"}
        doc_ids = {}
        for i, (person, gender) in enumerate(people.items(), start=1):
            row_id = uuid4()
            doc_id = uuid4()
            doc_ids[person] = doc_id
            session.add(
                DemographicRow(
                    id=row_id,
                    demographic_file_id=file_id,
                    corpus_id=corpus_id,
                    row_number=i,
                    interviewee_id=person,
                    data={"gender": gender},
                )
            )
            session.add(
                CorpusDocument(
                    id=doc_id,
                    corpus_id=corpus_id,
                    demographic_row_id=row_id,
                    title=person,
                    content="body",
                )
            )
        await session.flush()

        session.add(
            CodebookApplicationRun(
                id=run_id,
                corpus_id=corpus_id,
                codebook_id=codebook_id,
                status="succeeded",
                documents_total=len(people),
                documents_coded=len(people),
            )
        )
        await session.flush()
        for person, doc_id in doc_ids.items():
            coding_id = uuid4()
            session.add(
                DocumentCoding(
                    id=coding_id,
                    application_run_id=run_id,
                    document_id=doc_id,
                    codebook_id=codebook_id,
                    status="coded",
                )
            )
            await session.flush()
            session.add(
                ThemeAssignment(
                    id=uuid4(),
                    document_coding_id=coding_id,
                    theme_id=theme_id,
                    is_present=person in present,
                    confidence=0.9,
                )
            )
        await session.commit()
        return corpus_id, codebook_id, theme_id


async def test_dimensions_endpoint_lists_variables(client, db_engine):
    corpus_id, _, _ = await _seed(db_engine)
    response = await client.get(f"/api/v1/demographic/{corpus_id}/dimensions")
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["data"]["dimensions"] == ["gender"]


async def test_dimensions_endpoint_empty_when_no_data(client):
    response = await client.get(f"/api/v1/demographic/{uuid.uuid4()}/dimensions")
    assert response.status_code == 200
    assert response.json()["data"]["dimensions"] == []


async def test_breakdown_endpoint_returns_groups(client, db_engine):
    _, codebook_id, theme_id = await _seed(db_engine)
    response = await client.get(
        f"/api/v1/codebooks/{codebook_id}/themes/{theme_id}/demographic-breakdown",
        params={"dimensions": "gender"},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data["dimensions"]) == 1
    groups = {g["group_value"]: g for g in data["dimensions"][0]["groups"]}
    assert groups["male"]["present_count"] == 1
    assert groups["male"]["group_total"] == 2
    assert groups["male"]["percentage"] == 50.0
    assert groups["female"]["present_count"] == 2
    assert groups["female"]["percentage"] == 100.0


async def test_breakdown_endpoint_unknown_codebook_404(client):
    response = await client.get(
        f"/api/v1/codebooks/{uuid.uuid4()}/themes/{uuid.uuid4()}/demographic-breakdown",
        params={"dimensions": "gender"},
    )
    assert response.status_code == 404


async def test_breakdown_endpoint_no_dimensions_returns_empty(client, db_engine):
    _, codebook_id, theme_id = await _seed(db_engine)
    response = await client.get(
        f"/api/v1/codebooks/{codebook_id}/themes/{theme_id}/demographic-breakdown",
    )
    assert response.status_code == 200
    assert response.json()["data"]["dimensions"] == []
