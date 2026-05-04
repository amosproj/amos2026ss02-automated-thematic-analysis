from __future__ import annotations

from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Codebook, CodebookThemeRelationship, Theme


async def _seed_codebook_with_themes(db_engine):
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        codebook = Codebook(
            id=uuid4(),
            project_id="project_frequency_api",
            name="Theme Frequency API",
            description="Fixture",
            version=1,
            created_by="system",
        )
        themes = [
            Theme(id=uuid4(), label="Alpha Theme", is_active=True),
            Theme(id=uuid4(), label="Beta Theme", is_active=True),
            Theme(id=uuid4(), label="Gamma Theme", is_active=True),
        ]
        session.add(codebook)
        session.add_all(themes)
        await session.flush()

        for theme in themes:
            session.add(
                CodebookThemeRelationship(
                    id=uuid4(),
                    codebook_id=codebook.id,
                    theme_id=theme.id,
                    is_active=True,
                )
            )
        await session.commit()
        return codebook.id


async def test_list_themes_returns_zero_frequency_rows(client, db_engine):
    codebook_id = await _seed_codebook_with_themes(db_engine)
    response = await client.get(f"/api/v1/codebooks/{codebook_id}/themes")

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert len(payload["data"]) == 3
    assert all(row["occurrence_count"] == 0 for row in payload["data"])
    assert all(row["interview_coverage_percentage"] == 0 for row in payload["data"])
    assert [row["theme_name"] for row in payload["data"]] == [
        "Alpha Theme",
        "Beta Theme",
        "Gamma Theme",
    ]


async def test_list_themes_returns_404_for_unknown_codebook(client):
    response = await client.get(f"/api/v1/codebooks/{uuid4()}/themes")
    assert response.status_code == 404
