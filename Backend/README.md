# Backend

FastAPI backend — async SQLAlchemy 2.x, Pydantic v2, Loguru.

## Quick Start

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

## Project Structure

```
app/
├── main.py            # Application factory + lifespan
├── config.py          # Pydantic Settings (from .env)
├── database.py        # Async SQLAlchemy engine + session
├── dependencies.py    # FastAPI dependency injection aliases
├── exceptions.py      # Custom exceptions + handlers
├── logging_config.py  # Loguru setup + stdlib bridge
├── middleware.py      # RequestId, logging, CORS, GZip
├── models/            # SQLAlchemy ORM models (add here)
├── routers/           # FastAPI APIRouters (add here)
├── schemas/           # Pydantic request/response schemas
└── services/          # Business logic layer (add here)
```

## Response Format

All endpoints return a `ResponseEnvelope`:

```json
{ "success": true,  "data": { ... }, "error": null, "meta": null }
{ "success": false, "data": null,    "error": "...", "meta": { "detail": "..." } }
```

## Environment Variables

See `.env.example` — every variable the app reads is documented there.
