import logging
import sys

from loguru import logger

from app.config import Settings


class _InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back  # type: ignore[assignment]
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def configure_logging(settings: Settings) -> None:
    logger.remove()

    if settings.LOG_FORMAT == "json":
        logger.add(sys.stdout, level=settings.LOG_LEVEL, serialize=True, enqueue=True)
    else:
        logger.add(
            sys.stdout,
            level=settings.LOG_LEVEL,
            colorize=True,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
            enqueue=True,
        )

    intercept = _InterceptHandler()
    logging.basicConfig(handlers=[intercept], level=0, force=True)
    for name in ("uvicorn", "uvicorn.access", "uvicorn.error", "sqlalchemy.engine", "fastapi"):
        log = logging.getLogger(name)
        log.handlers = [intercept]
        log.propagate = False
