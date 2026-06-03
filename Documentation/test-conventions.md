# Backend Test Conventions

Guidelines and conventions for writing backend tests. Based on lessons learned during the test suite stabilization.

## Test Infrastructure

### Database

- Tests run against an **in-memory SQLite** database (`sqlite+aiosqlite:///:memory:`), not PostgreSQL.
- The `conftest.py` fixture `db_engine` creates all tables at test start and drops them at teardown.
- The `client` fixture provides a fully wired `httpx.AsyncClient` with the app's session dependency overridden.

### UUID Column Type

All SQLAlchemy models use `sqlalchemy.Uuid(as_uuid=True)` — the generic, dialect-agnostic UUID type. This ensures models work with both PostgreSQL (production) and SQLite (tests).

> **Do NOT** use `sqlalchemy.dialects.postgresql.UUID` in model definitions. It will break SQLite-based tests.

## Common Pitfalls

### 1. `from __future__ import annotations` must be first

If a test file uses `from __future__ import annotations`, it **must** be the very first statement. Placing `import uuid` before it causes a `SyntaxError`.

```python
# ✅ Correct
from __future__ import annotations

import uuid

# ❌ Wrong — SyntaxError
import uuid
from __future__ import annotations
```

### 2. Always `import uuid` when using `uuid.UUID(...)`

If you call `uuid.UUID("...")` or `uuid.uuid4()`, you must have `import uuid` in the file. Even if `from __future__ import annotations` is present, the runtime still needs the `uuid` module.

### 3. Use unique corpus IDs per test

The `Corpus.id` column is the primary key. In tests, the `corpus_id` field in the API payload becomes the `id` column directly. If two tests try to create corpora with the same `corpus_id`, the second will fail with a UNIQUE constraint violation.

```python
# ✅ Correct — unique per call
async def _create_corpus(client, name="Test"):
    resp = await client.post(
        "/api/v1/ingestion/corpora",
        json={"corpus_id": str(uuid.uuid4()), "name": name},
    )
    return resp.json()["data"]["id"]

# ❌ Wrong — hardcoded ID causes collisions when called multiple times
CORPUS_ID = "00000000-0000-0000-0000-000000000001"
async def _create_corpus(client, name="Test"):
    resp = await client.post(
        "/api/v1/ingestion/corpora",
        json={"corpus_id": CORPUS_ID, "name": name},
    )
```

### 4. Corpus must exist before creating codebooks

Codebooks have a foreign key to `corpora.id`. In integration tests using the full app stack, you must create a corpus via the ingestion API before creating a codebook.

### 5. Match service return signatures

When calling service methods, make sure to unpack the correct number of return values:

| Method | Returns |
|--------|---------|
| `CodebookService.create_codebook(...)` | 4-tuple: `(codebook, themes, hierarchy_edges, codes)` |
| `CodebookService.get_codebook_detail(...)` | 5-tuple: `(codebook, themes, hierarchy_edges, codes, theme_code_edges)` |

### 6. Raw SQL must match actual column names

When writing raw SQL in tests (e.g., for constraint tests), use the actual SQLAlchemy column names, not API field names:

| Table | Column | Not |
|-------|--------|----|
| `corpora` | `project_id` | ~~`corpus_id`~~ |
| `codebooks` | `corpus_id` | ~~`project_id`~~ |

### 7. Minimum node counts for codebook creation

The codebook creation endpoint validates that the number of nodes is between 1 and 50. Some earlier tests assumed lower limits (e.g., 2 themes). Ensure test payloads satisfy the `MIN_THEMES` / `MAX_THEMES` constraints.

## Running Tests

```bash
# Run full test suite
uv run python -m pytest tests -v

# Run a specific test file
uv run python -m pytest tests/test_codebook_router.py -v

# Run with output capture disabled (for debugging)
uv run python -m pytest tests/test_codebook_router.py -v -s
```

## Test Categories

| File | What it tests |
|------|---------------|
| `test_codebook_router.py` | Codebook CRUD API endpoints (CSV parse, create, detail, list) |
| `test_codebook_service.py` | `CodebookService` business logic |
| `test_codebook_generation_api.py` | LLM-based codebook generation endpoints |
| `test_demographic_api.py` | Demographic CSV upload, confirm, and listing |
| `test_ingestion_api.py` | Document ingestion API endpoints |
| `test_ingestion_service.py` | `IngestionService` business logic |
| `test_router_units.py` | Unit tests for router-level logic (mocked sessions) |
| `test_model_schema_alignment.py` | SQLAlchemy model ↔ Pydantic schema alignment |
| `test_theme_graph_service.py` | Theme tree (DAG) construction and validation |
| `test_theme_frequency_service.py` | Theme frequency calculation |
| `test_upload_parsing.py` | File format parsing (JSONL, TXT, DOCX, PDF) |
| `test_text_chunking.py` | Text chunking algorithm |
