from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from app.services.upload_cleanup import (
    delete_files_older_than,
    prune_empty_directories,
    purge_uploads_directory,
    run_upload_cleanup_loop,
)


def test_purge_uploads_directory_recreates_clean_tree(tmp_path: Path) -> None:
    uploads = tmp_path / "uploads"
    nested = uploads / "a" / "b"
    nested.mkdir(parents=True)
    (nested / "data.csv").write_text("x", encoding="utf-8")

    purge_uploads_directory(uploads)

    assert uploads.exists()
    assert uploads.is_dir()
    assert list(uploads.rglob("*")) == []


def test_delete_files_older_than_missing_directory_returns_zero(tmp_path: Path) -> None:
    assert delete_files_older_than(tmp_path / "does-not-exist", max_age_seconds=10) == 0


def test_delete_files_older_than_removes_only_expired_files(tmp_path: Path) -> None:
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    old_file = uploads / "old.csv"
    new_file = uploads / "new.csv"
    old_file.write_text("old", encoding="utf-8")
    new_file.write_text("new", encoding="utf-8")

    now = time.time()
    old_mtime = now - 120
    new_mtime = now - 5
    old_file.touch()
    new_file.touch()
    import os
    os.utime(old_file, (old_mtime, old_mtime))
    os.utime(new_file, (new_mtime, new_mtime))

    deleted = delete_files_older_than(uploads, max_age_seconds=60)
    assert deleted == 1
    assert not old_file.exists()
    assert new_file.exists()


def test_prune_empty_directories_removes_only_empty_dirs(tmp_path: Path) -> None:
    uploads = tmp_path / "uploads"
    empty_branch = uploads / "empty" / "deep"
    non_empty = uploads / "keep"
    empty_branch.mkdir(parents=True)
    non_empty.mkdir(parents=True)
    (non_empty / "x.txt").write_text("content", encoding="utf-8")

    prune_empty_directories(uploads)

    assert not empty_branch.exists()
    assert (uploads / "empty").exists() is False
    assert non_empty.exists()


@pytest.mark.asyncio
async def test_run_upload_cleanup_loop_calls_cleanup_functions(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    def _delete(_uploads_dir: Path, _max_age: int) -> int:
        calls.append("delete")
        return 0

    def _prune(_uploads_dir: Path) -> None:
        calls.append("prune")

    async def _sleep(_seconds: int) -> None:
        raise asyncio.CancelledError()

    monkeypatch.setattr("app.services.upload_cleanup.delete_files_older_than", _delete)
    monkeypatch.setattr("app.services.upload_cleanup.prune_empty_directories", _prune)
    monkeypatch.setattr("app.services.upload_cleanup.asyncio.sleep", _sleep)

    with pytest.raises(asyncio.CancelledError):
        await run_upload_cleanup_loop(tmp_path / "uploads", max_age_seconds=60, interval_seconds=1)

    assert calls == ["delete", "prune"]
