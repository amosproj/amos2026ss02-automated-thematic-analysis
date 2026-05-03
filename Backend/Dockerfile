# Stage 1: install runtime dependencies
FROM python:3.11-slim AS builder-runtime

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /build

COPY pyproject.toml .
RUN uv pip install --system --no-cache .

# Stage 2: install test/dev dependencies
FROM python:3.11-slim AS builder-test

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /build

COPY pyproject.toml .
RUN uv pip install --system --no-cache ".[dev]"

# Stage 3: production runtime
FROM python:3.11-slim AS runtime

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

COPY --from=builder-runtime /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder-runtime /usr/local/bin/uvicorn /usr/local/bin/uvicorn
COPY app/ app/
# COPY migrations/ migrations/
# COPY alembic.ini .

RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/health/')"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Stage 4: test runner image
FROM python:3.11-slim AS test

WORKDIR /app

COPY --from=builder-test /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder-test /usr/local/bin/pytest /usr/local/bin/pytest
COPY app/ app/
COPY tests/ tests/

CMD ["pytest"]
