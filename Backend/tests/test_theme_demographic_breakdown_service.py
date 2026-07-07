from __future__ import annotations

import importlib.util
import unittest
from dataclasses import dataclass, field
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.models import (
    Base,
    Codebook,
    CodebookApplicationRun,
    CodebookThemeRelationship,
    CorpusDocument,
    DemographicFiles,
    DemographicRow,
    DocumentCoding,
    Theme,
    ThemeAssignment,
    ThemeHierarchyRelationship,
)
from app.services.theme_demographic_breakdown import (
    NOT_SPECIFIED_LABEL,
    ThemeDemographicBreakdownService,
)
from app.services.theme_graph import ThemeNotFoundError

AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None


@dataclass
class BreakdownSeed:
    corpus_id: UUID
    codebook_id: UUID
    run_id: UUID
    theme_ids: dict[str, UUID] = field(default_factory=dict)


async def _seed_breakdown(
    session: AsyncSession,
    *,
    columns: list[str],
    # interviewee -> demographic data dict (or None to leave the transcript unlinked)
    people: dict[str, dict | None],
    theme_labels: list[str],
    # theme label -> set of interviewees for whom the theme is present
    present_by_theme: dict[str, set[str]],
    run_status: str = "succeeded",
    # optional parent->child hierarchy edges by theme label
    edges_by_label: list[tuple[str, str]] | None = None,
) -> BreakdownSeed:
    corpus_id = uuid4()
    codebook_id = uuid4()
    run_id = uuid4()

    session.add(
        Codebook(
            id=codebook_id,
            corpus_id=corpus_id,
            name="Breakdown Codebook",
            description="Fixture",
            version=1,
            created_by="system",
        )
    )
    theme_ids = {label: uuid4() for label in theme_labels}
    for label, theme_id in theme_ids.items():
        session.add(Theme(id=theme_id, codebook_id=codebook_id, label=label, is_active=True))
    await session.flush()
    for theme_id in theme_ids.values():
        session.add(
            CodebookThemeRelationship(
                id=uuid4(), codebook_id=codebook_id, theme_id=theme_id, is_active=True
            )
        )
    for parent_label, child_label in edges_by_label or []:
        session.add(
            ThemeHierarchyRelationship(
                id=uuid4(),
                codebook_id=codebook_id,
                parent_theme_id=theme_ids[parent_label],
                child_theme_id=theme_ids[child_label],
                is_active=True,
            )
        )

    demographic_file_id = uuid4()
    session.add(
        DemographicFiles(
            id=demographic_file_id,
            name="people.csv",
            corpus_id=corpus_id,
            original_columns=columns,
        )
    )

    document_ids: dict[str, UUID] = {}
    for row_number, (interviewee, data) in enumerate(people.items(), start=1):
        document_id = uuid4()
        document_ids[interviewee] = document_id
        demographic_row_id: UUID | None = None
        if data is not None:
            demographic_row_id = uuid4()
            session.add(
                DemographicRow(
                    id=demographic_row_id,
                    demographic_file_id=demographic_file_id,
                    corpus_id=corpus_id,
                    row_number=row_number,
                    interviewee_id=interviewee,
                    data=data,
                )
            )
        session.add(
            CorpusDocument(
                id=document_id,
                corpus_id=corpus_id,
                demographic_row_id=demographic_row_id,
                title=interviewee,
                content="transcript body",
            )
        )
    await session.flush()

    session.add(
        CodebookApplicationRun(
            id=run_id,
            corpus_id=corpus_id,
            codebook_id=codebook_id,
            status=run_status,
            documents_total=len(people),
            documents_coded=len(people),
        )
    )
    await session.flush()

    for interviewee, document_id in document_ids.items():
        coding_id = uuid4()
        session.add(
            DocumentCoding(
                id=coding_id,
                application_run_id=run_id,
                document_id=document_id,
                codebook_id=codebook_id,
                status="coded",
            )
        )
        await session.flush()
        for label, theme_id in theme_ids.items():
            session.add(
                ThemeAssignment(
                    id=uuid4(),
                    document_coding_id=coding_id,
                    theme_id=theme_id,
                    is_present=interviewee in present_by_theme.get(label, set()),
                    confidence=0.9,
                )
            )

    await session.commit()
    return BreakdownSeed(
        corpus_id=corpus_id,
        codebook_id=codebook_id,
        run_id=run_id,
        theme_ids=theme_ids,
    )


@unittest.skipUnless(AIOSQLITE_AVAILABLE, "These tests require aiosqlite.")
class ThemeDemographicBreakdownServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    # --- available dimensions ------------------------------------------------

    async def test_lists_available_dimensions_excluding_username(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender", "age_group", "party"],
                people={"p1": {"gender": "male", "age_group": "18-29", "party": "A"}},
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"p1"}},
            )
            dims = await ThemeDemographicBreakdownService(session).list_available_dimensions(
                corpus_id=seed.corpus_id
            )
            self.assertEqual(dims, ["gender", "age_group", "party"])

    async def test_no_demographic_data_yields_no_dimensions(self) -> None:
        async with self.session_factory() as session:
            # A corpus with no demographic files at all.
            codebook_id = uuid4()
            corpus_id = uuid4()
            session.add(
                Codebook(
                    id=codebook_id,
                    corpus_id=corpus_id,
                    name="No Demo",
                    description="Fixture",
                    version=1,
                    created_by="system",
                )
            )
            await session.commit()
            dims = await ThemeDemographicBreakdownService(session).list_available_dimensions(
                corpus_id=corpus_id
            )
            self.assertEqual(dims, [])

    # --- binary dimension (DoD: gender male/female) --------------------------

    async def test_binary_dimension_counts_and_percentages(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender"],
                people={
                    "m1": {"gender": "male"},
                    "m2": {"gender": "male"},
                    "f1": {"gender": "female"},
                    "f2": {"gender": "female"},
                },
                theme_labels=["Theme A"],
                # 50% of males, 100% of females
                present_by_theme={"Theme A": {"m1", "f1", "f2"}},
            )
            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Theme A"],
                dimensions=["gender"],
            )
            self.assertEqual(len(result.dimensions), 1)
            groups = {g.group_value: g for g in result.dimensions[0].groups}
            self.assertEqual(groups["male"].present_count, 1)
            self.assertEqual(groups["male"].group_total, 2)
            self.assertEqual(groups["male"].percentage, 50.0)
            self.assertEqual(groups["female"].present_count, 2)
            self.assertEqual(groups["female"].group_total, 2)
            self.assertEqual(groups["female"].percentage, 100.0)

    async def test_zero_occurrence_group_shown_with_count_zero(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender"],
                people={"m1": {"gender": "male"}, "f1": {"gender": "female"}},
                # Theme present for nobody female
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"m1"}},
            )
            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Theme A"],
                dimensions=["gender"],
            )
            groups = {g.group_value: g for g in result.dimensions[0].groups}
            self.assertIn("female", groups)
            self.assertEqual(groups["female"].present_count, 0)
            self.assertEqual(groups["female"].percentage, 0.0)

    # --- multi-value dimension (DoD: political affiliation 4+ values) --------

    async def test_multi_value_dimension(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "party"],
                people={
                    "a": {"party": "Green"},
                    "b": {"party": "Liberal"},
                    "c": {"party": "Conservative"},
                    "d": {"party": "Socialist"},
                    "e": {"party": "Green"},
                },
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"a", "c"}},
            )
            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Theme A"],
                dimensions=["party"],
            )
            groups = {g.group_value: g for g in result.dimensions[0].groups}
            self.assertEqual(set(groups), {"Green", "Liberal", "Conservative", "Socialist"})
            self.assertEqual(groups["Green"].group_total, 2)
            self.assertEqual(groups["Green"].present_count, 1)
            self.assertEqual(groups["Green"].percentage, 50.0)
            self.assertEqual(groups["Liberal"].present_count, 0)

    # --- multiple dimensions at once (DoD: 1, 3, 5+ dimensions) --------------

    async def test_multiple_dimensions_returned_in_request_order(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender", "age_group", "party", "region", "education"],
                people={
                    "p1": {
                        "gender": "male",
                        "age_group": "18-29",
                        "party": "Green",
                        "region": "North",
                        "education": "BSc",
                    },
                    "p2": {
                        "gender": "female",
                        "age_group": "30-44",
                        "party": "Liberal",
                        "region": "South",
                        "education": "MSc",
                    },
                },
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"p1"}},
            )
            requested = ["party", "gender", "education", "region", "age_group"]
            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Theme A"],
                dimensions=requested,
            )
            self.assertEqual([d.dimension for d in result.dimensions], requested)

    async def test_unknown_dimension_is_ignored(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender"],
                people={"p1": {"gender": "male"}},
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"p1"}},
            )
            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Theme A"],
                dimensions=["gender", "not_a_real_column"],
            )
            self.assertEqual([d.dimension for d in result.dimensions], ["gender"])

    # --- missing / unlinked data --------------------------------------------

    async def test_unlinked_and_blank_values_bucket_into_not_specified(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender"],
                people={
                    "linked": {"gender": "male"},
                    "blank": {"gender": "  "},
                    "missing_key": {"age": "40"},
                    "unlinked": None,
                },
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"linked", "unlinked"}},
            )
            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Theme A"],
                dimensions=["gender"],
            )
            groups = {g.group_value: g for g in result.dimensions[0].groups}
            self.assertEqual(groups[NOT_SPECIFIED_LABEL].group_total, 3)
            self.assertEqual(groups[NOT_SPECIFIED_LABEL].present_count, 1)
            # Not specified is sorted last.
            self.assertEqual(result.dimensions[0].groups[-1].group_value, NOT_SPECIFIED_LABEL)

    # --- small sample --------------------------------------------------------

    async def test_small_sample_flagged_below_threshold(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender"],
                people={
                    "m1": {"gender": "male"},
                    "m2": {"gender": "male"},
                    "m3": {"gender": "male"},
                    "m4": {"gender": "male"},
                    "m5": {"gender": "male"},
                    "f1": {"gender": "female"},
                    "f2": {"gender": "female"},
                },
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"m1"}},
            )
            result = await ThemeDemographicBreakdownService(
                session, small_sample_threshold=5
            ).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Theme A"],
                dimensions=["gender"],
            )
            groups = {g.group_value: g for g in result.dimensions[0].groups}
            self.assertFalse(groups["male"].small_sample)  # total 5, not < 5
            self.assertTrue(groups["female"].small_sample)  # total 2 < 5

    # --- run handling / errors ----------------------------------------------

    async def test_no_run_returns_empty_groups_for_selected_dimensions(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender"],
                people={"m1": {"gender": "male"}},
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"m1"}},
                run_status="failed",  # no succeeded run to default to
            )
            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Theme A"],
                dimensions=["gender"],
            )
            self.assertIsNone(result.application_run_id)
            self.assertEqual(len(result.dimensions), 1)
            self.assertEqual(result.dimensions[0].groups, [])

    async def test_unknown_codebook_raises(self) -> None:
        async with self.session_factory() as session:
            with self.assertRaises(ThemeNotFoundError):
                await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                    codebook_id=uuid4(),
                    theme_id=uuid4(),
                    dimensions=["gender"],
                )

    async def test_unknown_theme_raises(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "gender"],
                people={"m1": {"gender": "male"}},
                theme_labels=["Theme A"],
                present_by_theme={"Theme A": {"m1"}},
            )
            with self.assertRaises(ThemeNotFoundError):
                await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                    codebook_id=seed.codebook_id,
                    theme_id=uuid4(),
                    dimensions=["gender"],
                )

    async def test_parent_theme_rolls_up_descendant_present_counts(self) -> None:
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "Region"],
                people={
                    "Alice": {"Region": "North"},
                    "Bob": {"Region": "North"},
                    "Carol": {"Region": "South"},
                    "Dave": {"Region": "South"},
                },
                theme_labels=["Parent", "Child A", "Child B"],
                present_by_theme={
                    "Child A": {"Alice", "Bob"},
                    "Child B": {"Bob", "Carol"},
                    "Parent": set(),
                },
                edges_by_label=[("Parent", "Child A"), ("Parent", "Child B")],
            )

            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Parent"],
                dimensions=["Region"],
            )

        groups = {g.group_value: g for g in result.dimensions[0].groups}
        # North: Alice + Bob present (Bob once) out of 2 → 100%.
        self.assertEqual(groups["North"].present_count, 2)
        self.assertEqual(groups["North"].group_total, 2)
        self.assertEqual(groups["North"].percentage, 100.0)
        # South: only Carol (via Child B) out of 2 → 50%.
        self.assertEqual(groups["South"].present_count, 1)
        self.assertEqual(groups["South"].group_total, 2)
        self.assertEqual(groups["South"].percentage, 50.0)

    async def test_leaf_theme_breakdown_is_not_affected_by_rollup(self) -> None:
        # Selecting a child theme still counts only its own assignments.
        async with self.session_factory() as session:
            seed = await _seed_breakdown(
                session,
                columns=["username", "Region"],
                people={
                    "Alice": {"Region": "North"},
                    "Bob": {"Region": "North"},
                    "Carol": {"Region": "South"},
                    "Dave": {"Region": "South"},
                },
                theme_labels=["Parent", "Child A", "Child B"],
                present_by_theme={
                    "Child A": {"Alice", "Bob"},
                    "Child B": {"Bob", "Carol"},
                    "Parent": set(),
                },
                edges_by_label=[("Parent", "Child A"), ("Parent", "Child B")],
            )

            result = await ThemeDemographicBreakdownService(session).get_theme_breakdown(
                codebook_id=seed.codebook_id,
                theme_id=seed.theme_ids["Child A"],
                dimensions=["Region"],
            )

        groups = {g.group_value: g for g in result.dimensions[0].groups}
        # Child A alone: Alice + Bob (North); South has none.
        self.assertEqual(groups["North"].present_count, 2)
        self.assertEqual(groups["South"].present_count, 0)
        self.assertEqual(groups["South"].percentage, 0.0)


if __name__ == "__main__":
    unittest.main()
