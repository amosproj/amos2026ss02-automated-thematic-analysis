import asyncio
import contextlib
import shutil
import time
from pathlib import Path

from loguru import logger


def purge_uploads_directory(uploads_dir: Path) -> None:
    """Delete the uploads directory and all of its contents, then recreate it."""
    if uploads_dir.exists():
        shutil.rmtree(uploads_dir)
        logger.info("Purged uploads directory at {}", uploads_dir)
    uploads_dir.mkdir(parents=True, exist_ok=True)


def delete_files_older_than(uploads_dir: Path, max_age_seconds: int) -> int:
    """Delete files older than max_age_seconds. Returns the number of deleted files."""
    if not uploads_dir.exists():
        return 0

    now = time.time()
    deleted = 0
    for path in uploads_dir.rglob("*"):
        if not path.is_file():
            continue
        age_seconds = now - path.stat().st_mtime
        if age_seconds > max_age_seconds:
            path.unlink(missing_ok=True)
            deleted += 1

    if deleted:
        logger.info(
            "Deleted {} expired upload files older than {} seconds",
            deleted,
            max_age_seconds,
        )
    return deleted


def prune_empty_directories(uploads_dir: Path) -> None:
    """Remove empty directories under uploads_dir."""
    if not uploads_dir.exists():
        return
    for directory in sorted(
        (p for p in uploads_dir.rglob("*") if p.is_dir()),
        key=lambda p: len(p.parts),
        reverse=True,
    ):
        with contextlib.suppress(OSError):
            directory.rmdir()


async def run_upload_cleanup_loop(
    uploads_dir: Path,
    max_age_seconds: int,
    interval_seconds: int,
) -> None:
    """Background loop that periodically removes expired upload files."""
    while True:
        delete_files_older_than(uploads_dir, max_age_seconds)
        prune_empty_directories(uploads_dir)
        await asyncio.sleep(interval_seconds)
