from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.exceptions import NotFoundError, UnprocessableError
from app.models import Base, Codebook
from app.routers import codebooks as codebooks_router
from app.routers import demo as demo_router
from app.routers import themes as themes_router
from app.services.theme_graph import ThemeGraphService, ThemeNotFoundError, ThemeValidationError


AIOSQLITE_AVAILABLE = importlib.util.find_spec("aiosqlite") is not None


@unittest.skipUnless(
    AIOSQLITE_AVAILABLE,
    "These tests require aiosqlite.",
)
class RouterUnitTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _build_request(path: str) -> Request:
        app = FastAPI()
        static_dir = Path(__file__).resolve().parents[1] / "app" / "static"
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
        return Request(
            scope={
                "type": "http",
                "method": "GET",
                "path": path,
                "headers": [],
                "query_string": b"",
                "app": app,
                "router": app.router,
            }
        )

    async def asyncSetUp(self) -> None:
        self.engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def asyncTearDown(self) -> None:
        await self.engine.dispose()

    async def test_codebooks_route_returns_sorted_payload(self) -> None:
        async with self.session_factory() as session:
            session.add_all(
                [
                    Codebook(
                        id=uuid4(),
                        project_id="project_b",
                        name="B v1",
                        description="desc",
                        version=1,
                        created_by="system",
                    ),
                    Codebook(
                        id=uuid4(),
                        project_id="project_a",
                        name="A v1",
                        description="desc",
                        version=1,
                        created_by="system",
                    ),
                    Codebook(
                        id=uuid4(),
                        project_id="project_a",
                        name="A v2",
                        description="desc",
                        version=2,
                        created_by="system",
                    ),
                ]
            )
            await session.commit()

            response = await codebooks_router.get_codebooks(session=session)
            payload = json.loads(response.body)
            ordered_pairs = [(row["project_id"], row["version"]) for row in payload["data"]]
            self.assertEqual(
                ordered_pairs,
                [("project_a", 2), ("project_a", 1), ("project_b", 1)],
            )

    async def test_themes_route_maps_not_found_error(self) -> None:
        async with self.session_factory() as session:
            with patch.object(
                ThemeGraphService,
                "get_theme_tree",
                new=AsyncMock(side_effect=ThemeNotFoundError("missing")),
            ):
                with self.assertRaises(NotFoundError):
                    await themes_router.get_theme_tree(
                        codebook_id=uuid4(),
                        session=session,
                        root_theme_id=None,
                    )

    async def test_themes_route_maps_validation_error(self) -> None:
        async with self.session_factory() as session:
            with patch.object(
                ThemeGraphService,
                "get_theme_tree",
                new=AsyncMock(side_effect=ThemeValidationError("invalid hierarchy")),
            ):
                with self.assertRaises(UnprocessableError):
                    await themes_router.get_theme_tree(
                        codebook_id=uuid4(),
                        session=session,
                        root_theme_id=None,
                    )

    async def test_themes_route_success_response_shape(self) -> None:
        async with self.session_factory() as session:
            with patch.object(
                ThemeGraphService,
                "get_theme_tree",
                new=AsyncMock(return_value=[]),
            ):
                response = await themes_router.get_theme_tree(
                    codebook_id=uuid4(),
                    session=session,
                    root_theme_id=None,
                )
            payload = json.loads(response.body)
            self.assertTrue(payload["success"])
            self.assertIn("data", payload)
            self.assertEqual(payload["data"], [])

    async def test_demo_overview_404s_when_project_or_version_missing(self) -> None:
        async with self.session_factory() as session:
            request = self._build_request("/demo/overview")
            with patch.object(
                demo_router,
                "get_settings",
                return_value=SimpleNamespace(API_V1_PREFIX="/api/v1"),
            ):
                with self.assertRaises(HTTPException) as missing_project:
                    await demo_router.theme_overview_screen(
                        request=request,
                        session=session,
                        project_id="unknown_project",
                        version=None,
                    )
                self.assertEqual(missing_project.exception.status_code, 404)

                session.add(
                    Codebook(
                        id=uuid4(),
                        project_id="project_demo",
                        name="Demo v1",
                        description="desc",
                        version=1,
                        created_by="system",
                    )
                )
                await session.commit()
                with self.assertRaises(HTTPException) as missing_version:
                    await demo_router.theme_overview_screen(
                        request=request,
                        session=session,
                        project_id="project_demo",
                        version=99,
                    )
                self.assertEqual(missing_version.exception.status_code, 404)

    async def test_demo_overview_defaults_to_latest_version(self) -> None:
        async with self.session_factory() as session:
            request = self._build_request("/demo/overview")
            session.add_all(
                [
                    Codebook(
                        id=uuid4(),
                        project_id="project_demo_latest",
                        name="Demo Latest v1",
                        description="desc",
                        version=1,
                        created_by="system",
                    ),
                    Codebook(
                        id=uuid4(),
                        project_id="project_demo_latest",
                        name="Demo Latest v2",
                        description="desc",
                        version=2,
                        created_by="system",
                    ),
                ]
            )
            await session.commit()
            with patch.object(
                demo_router,
                "get_settings",
                return_value=SimpleNamespace(API_V1_PREFIX="/api/v1"),
            ):
                response = await demo_router.theme_overview_screen(
                    request=request,
                    session=session,
                    project_id="project_demo_latest",
                    version=None,
                )
            self.assertEqual(response.context["selected_version"], 2)
            self.assertEqual(response.context["selected_codebook_name"], "Demo Latest v2")
