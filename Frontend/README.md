# Frontend

Flask + Jinja2 server-rendered frontend. Talks to the FastAPI backend over internal REST/JSON.

## Layout

```
Frontend/
├── web/
│   ├── __init__.py                    # Flask app factory — registers all blueprints
│   ├── config.py                      # Pydantic-settings config (env vars, upload limits)
│   ├── controllers/
│   │   ├── main.py                    # / (home), /health
│   │   ├── ingestion.py               # /transcripts, /transcripts/upload
│   │   ├── codebooks.py               # /codebooks, /codebooks/<id>/themes
│   │   └── analysis.py                # /analysis (placeholder)
│   ├── services/
│   │   └── backend_client.py          # HTTP client wrapping all FastAPI calls
│   ├── templates/
│   │   ├── base.html                  # Shared layout — Bootstrap 5.3.3, top nav
│   │   ├── index.html                 # Home page with 3-card quick-links
│   │   ├── ingestion/
│   │   │   ├── upload.html            # Multi-file upload form
│   │   │   ├── list.html              # Transcript list
│   │   │   └── results.html           # Upload results summary
│   │   ├── codebooks/
│   │   │   ├── list.html              # Codebook listing table
│   │   │   └── themes.html            # Theme browser (table + tree + detail panel)
│   │   └── analysis/
│   │       └── index.html             # Analysis page (placeholder)
│   └── static/
│       ├── css/main.css               # All custom styles
│       └── js/codebook_themes.js      # Vanilla JS for the theme browser
├── tests/
│   ├── conftest.py                    # Shared fixtures — FakeBackend, app, client
│   ├── test_smoke.py                  # Basic route smoke tests
│   ├── test_ingestion.py              # Ingestion route tests
│   └── test_codebooks.py              # Codebook route tests
├── Dockerfile                         # Multi-stage build (runtime + test targets)
├── pyproject.toml
└── .env.example
```

## Pages and routes

| Route | Page | Description |
|---|---|---|
| `GET /` | Home | Quick-link cards to Transcripts, Codebooks, Analysis |
| `GET /health` | — | Health check endpoint (used by Docker) |
| `GET /transcripts/` | Transcript list | Lists all uploaded documents in the corpus |
| `GET /transcripts/upload` | Upload form | Multi-file upload (`.txt`, `.docx`, `.pdf`, `.jsonl`) |
| `POST /transcripts/upload` | Upload results | Submits files to the backend; shows per-file result |
| `GET /codebooks/` | Codebook list | Table of all available codebooks |
| `GET /codebooks/<id>/themes` | Theme browser | Full interactive theme browser for a codebook |
| `GET /analysis/` | Analysis | Placeholder — ready for backend analysis routes |

## Theme browser

The theme browser (`/codebooks/<id>/themes`) is the main UI feature. It has three panels:

- **Theme Frequency table** — all themes sorted by occurrence count with a 3-color coverage progress bar (rose 0–33 %, amber 34–66 %, emerald 67–100 %)
- **Theme Hierarchy tree** — connector-line file-explorer style tree; starts collapsed; clicking a root row selects the theme and toggles children; clicking a child row selects only
- **Theme Details panel** — shows the theme name, UUID chip, occurrence count, and interview coverage for the selected theme

All data is embedded server-side as JSON in `data-` attributes on `#theme-app`. The JS in `codebook_themes.js` reads those attributes on load — no extra HTTP requests after page render.

## Backend client

`web/services/backend_client.py` wraps every FastAPI call. All HTTP errors are caught and re-raised as `BackendError`, which controllers render as an alert rather than crashing.

| Method | Backend endpoint |
|---|---|
| `ensure_corpus(project_id, name)` | `POST /corpora/` |
| `upload_files(corpus_id, files)` | `POST /corpora/{id}/documents` |
| `list_documents(corpus_id)` | `GET /corpora/{id}/documents` |
| `list_codebooks()` | `GET /codebooks/` |
| `get_theme_frequencies(codebook_id)` | `GET /codebooks/{id}/themes` |
| `get_theme_tree(codebook_id)` | `GET /codebooks/{id}/themes/tree` |

## Configuration

All config is read from environment variables (or a `.env` file). Key settings:

| Variable | Default | Description |
|---|---|---|
| `BACKEND_API_URL` | `http://localhost:8000/api/v1` | FastAPI base URL |
| `BACKEND_TIMEOUT_S` | `60.0` | HTTP request timeout in seconds |
| `SECRET_KEY` | `dev-secret` | Flask session secret — change in production |
| `APP_ENV` | `development` | Set to `production` to enable production mode |
| `MAX_UPLOAD_SIZE_MB` | `10` | Per-file upload size cap |
| `DEFAULT_PROJECT_ID` | `00000000-0000-0000-0000-000000000001` | Single-workspace MVP project ID |

## Running with Docker (recommended)

The full stack — database, backend API, and frontend — is defined in `docker-compose.yml` at the repository root. Run everything from the repo root:

```bash
# Build and start all services
docker compose up --build

# Rebuild only the frontend (e.g. after dependency changes)
docker compose build --no-cache frontend
docker compose up -d --no-deps frontend
```

> **Note:** On Windows, `docker compose up --build` can reuse a stale BuildKit cache layer and serve old static files. If styles or JS disappear after a rebuild, always use `--no-cache` for the frontend image.

Services started:

| Service | URL |
|---|---|
| Frontend (Flask) | http://localhost:3000 |
| Backend (FastAPI) | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |

## Running locally (without Docker)

```bash
cp .env.example .env
# Edit .env — set BACKEND_API_URL to point at your running backend

# Install with uv (recommended)
uv sync --extra dev

# Or with pip
pip install -e ".[dev]"

flask --app web:create_app run --debug --port 3000
```

Open http://localhost:3000.

## Running tests

Tests use a `FakeBackend` fixture that replaces the real `BackendClient` so no running backend is required.

```bash
# Locally
pytest

# Via Docker
docker compose run --rm frontend-test
```

The test suite covers:
- Smoke tests for all routes (home, health, transcripts, upload, codebooks, analysis)
- Codebook list — data rendered, empty state, backend error handling
- Theme browser — frequency data, name from query param, empty state, backend error handling
- Ingestion — upload flow, file size validation, transcript listing

## Dockerfile

`Frontend/Dockerfile` is a multi-stage build:

| Stage | Purpose |
|---|---|
| `builder-runtime` | Installs runtime dependencies only (via `uv`) |
| `builder-test` | Installs runtime + dev/test dependencies |
| `runtime` | Minimal production image — runs `flask run` on port 3000, non-root user, health check |
| `test` | Test runner image — runs `pytest` |

The backend has its own Dockerfile at `Backend/Dockerfile`. The frontend Dockerfile follows the same multi-stage pattern but is kept separate because the two services have completely different dependencies and base configurations.

## Jinja2 template note

Flask uses `filename=` in `url_for` for static files — not `path=` (which is the FastAPI/Starlette convention). When porting a template from the backend, swap the kwarg:

```html
<!-- FastAPI / Starlette -->
{{ url_for('static', path='css/main.css') }}

<!-- Flask -->
{{ url_for('static', filename='css/main.css') }}
```

Block names (`title`, `head_extra`, `content`, `scripts`) are aligned between both sides, so no other changes are needed.
