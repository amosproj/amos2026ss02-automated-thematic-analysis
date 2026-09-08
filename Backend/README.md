# Backend

FastAPI backend - async SQLAlchemy 2.x, Pydantic v2, Loguru.

## Quick Start

> **One-command setup:** From the repository root, run `./setup.sh` (Linux/macOS/Git Bash) or `.\setup.ps1` (Windows PowerShell). It handles everything below automatically. See the [root README](../README.md) for details.

### Prerequisites
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- PostgreSQL 16 (or Docker)

### Setup

```bash
cp .env.example .env
# Edit .env with your database credentials

# Install with uv (recommended)
uv sync

# Or with pip
pip install -e .
```

### Run locally

```bash
# Start Postgres
docker compose up db -d

# Start dev server
uvicorn app.main:app --reload
```

### Run with Docker

```bash
docker compose up --build
# API at http://localhost:8000
# Docs at http://localhost:8000/docs
```

### Local Generation Algorithms

Codebook generation uses the algorithm selected on the Home page. The server
default comes from `GENERATION_ALGORITHM` in `.env` and is
`app.generation.algorithms.traceable:create`. To add a local algorithm, copy
`research_algorithms/example.py`, edit its `generate()` method, and either set it
as the configured default or add it to the server-side algorithm registry. See
`research_algorithms/README.md` for the contract, selector integration, and
validation rules.

The included non-LLM trial is available as **Keyword Frequency (Demo)** in the
Home-page algorithm selector.

After changing `GENERATION_ALGORITHM` in the root Docker setup, recreate the API
container from the repository root so Compose reloads `Backend/.env`:

```bash
docker compose up -d --force-recreate api
```

You can instead run `./teardown.sh` followed by `./setup.sh`; this stops and
recreates the stack while preserving the database volume.

Validate local generation algorithms with:

```bash
docker compose run --rm api-test pytest tests/test_generation_plugins.py
```

### Docker: Dev vs Prod

- `api` service uses the `runtime` target (production-style image, no test tooling).
- `api-test` service uses the `test` target (includes dev/test dependencies like `pytest` and `pytest-cov`).

## Tests
Run tests inside Docker:

```bash
docker compose --profile test run --rm api-test pytest --cov=app --cov-report=term-missing --cov-report=html
```

The coverage HTML report is written to `Backend/htmlcov/` on your host via the `api-test` volume mount.
Open `Backend/htmlcov/index.html` in your browser after the test run.

## Project Structure

```
app/
|-- main.py            # Application factory + lifespan
|-- config.py          # Pydantic Settings (from .env)
|-- database.py        # Async SQLAlchemy engine + session
|-- dependencies.py    # FastAPI dependency injection aliases
|-- exceptions.py      # Custom exceptions + handlers
|-- logging_config.py  # Loguru setup + stdlib bridge
|-- middleware.py      # RequestId, logging, CORS, GZip
|-- models/            # SQLAlchemy ORM models (add here)
|-- routers/           # FastAPI APIRouters (add here)
|-- schemas/           # Pydantic request/response schemas
`-- services/          # Business logic layer (add here)
```

## Response Format

All endpoints return a `ResponseEnvelope`:

```json
{ "success": true,  "data": { ... }, "error": null, "meta": null }
{ "success": false, "data": null,    "error": "...", "meta": { "detail": "..." } }
```

## Environment Variables

See `.env.example` - every variable the app reads is documented there.
