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
│   │   └── analysis.py                # /analysis
│   ├── services/
│   │   └── backend_client.py          # HTTP client wrapping all FastAPI calls
│   ├── templates/
│   │   ├── base.html                  # Shared layout — Bootstrap 5.3.3, top nav, footer, flash, favicon
│   │   ├── _flash.html                # Dismissible flash partial (icons + auto-dismiss)
│   │   ├── index.html                 # Home page with 3-card quick-links
│   │   ├── errors/
│   │   │   ├── 404.html               # Branded page not found
│   │   │   └── 500.html               # Branded server error
│   │   ├── ingestion/
│   │   │   ├── upload.html            # Multi-file upload form (dynamic JS file list)
│   │   │   ├── list.html              # Transcript list
│   │   │   └── results.html           # Upload results summary
│   │   ├── codebooks/
│   │   │   ├── list.html              # Codebook listing table
│   │   │   └── themes.html            # Theme browser (table + tree + detail panel)
│   │   └── analysis/
│   │       ├── index.html             # Trigger analysis form (transcript + codebook selection)
│   │       └── wait.html              # Live progress page (polls job status)
│   └── static/
│       ├── css/main.css               # All custom styles
│       ├── img/team-logo.svg          # NIM+AMOS team logo (navbar + footer + favicon)
│       └── js/codebook_themes.js      # Vanilla JS for the theme browser
├── tests/
│   ├── conftest.py                    # Shared fixtures — FakeBackend (typed errors), app, client
│   ├── test_smoke.py                  # Basic route smoke tests
│   ├── test_ingestion.py              # Ingestion route tests (incl. typed-error paths)
│   ├── test_codebooks.py              # Codebook route tests (incl. typed-error paths)
│   ├── test_analysis.py               # Analysis trigger + wait page tests
│   ├── test_backend_client.py         # Unit tests for BackendClient exception categorisation
│   └── test_error_handlers.py         # Flask 404 / 413 / 500 handler tests
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
| `GET /analysis/` | Trigger Analysis | Select transcripts, codebook, and trigger a codebook application job |
| `POST /analysis/trigger` | — | Submits the form; creates a backend apply-job and redirects to the wait page |
| `GET /analysis/job/<job_id>` | Applying Codebook | Live progress page that polls job status until completion |
| `GET /analysis/job/<job_id>/status` | — | JSON endpoint returning current job status (used by the wait page) |

## Theme browser

The theme browser (`/codebooks/<id>/themes`) is the main UI feature. It has three panels:

- **Theme Frequency table** — all themes sorted by occurrence count with a 3-color coverage progress bar (rose 0–33 %, amber 34–66 %, emerald 67–100 %)
- **Theme Hierarchy tree** — connector-line file-explorer style tree; starts collapsed; clicking a root row selects the theme and toggles children; clicking a child row selects only
- **Theme Details panel** — shows the theme name, UUID chip, occurrence count, and interview coverage for the selected theme

All data is embedded server-side as JSON in `data-` attributes on `#theme-app`. The JS in `codebook_themes.js` reads those attributes on load — no extra HTTP requests after page render.

## Backend client

`web/services/backend_client.py` wraps every FastAPI call. HTTP and network errors are categorised into typed `BackendError` subclasses, each carrying a `user_message` attribute that controllers flash to the user.

The set of endpoints the client targets is documented authoritatively by the backend itself — see [`Backend/README.md`](../Backend/README.md) or the live Swagger docs at `http://localhost:8000/docs`. To see exactly which endpoint each `BackendClient` method calls, read `web/services/backend_client.py` directly — kept as a single short file precisely so it stays the one place to look.

## Error handling

A four-layer model: BackendClient categorises, controllers catch and flash, templates render error- vs empty-state, Flask handlers catch what escapes.

### Exception hierarchy

| Class | Raised when | User-facing message |
|---|---|---|
| `BackendError` (base) | Anything uncategorised (malformed JSON, missing keys) | "Something went wrong. Please try again." |
| `BackendUnavailableError` | Connect refused, DNS failure, read timeout | "We can't reach the analysis service right now. Please try again in a moment." |
| `BackendNotFoundError` | Backend returns HTTP 404 | "The requested item couldn't be found. It may have been deleted." |
| `BackendValidationError` | Backend returns HTTP 422 — parses FastAPI's structured `detail[].msg` per field | Per-field message, e.g. `"name: field required; themes: must contain at least 1 item"` |
| `BackendServerError` | Backend returns 500 / 502 / 503 / 504 | "The analysis service had a problem. The team has been notified." |

Every failed request is logged once at the BackendClient boundary with a level matching the exception class (`warning` for unavailable, `info` for not-found / validation, `error` for everything else).

### Flask error handlers

| Status | Behaviour |
|---|---|
| 404 | Renders `templates/errors/404.html` — branded page with navbar/footer intact |
| 413 | Flashes "Upload too large…" and redirects (303) to the referrer |
| Generic `Exception` | Logs full traceback, renders `templates/errors/500.html` |

### Template convention

Every page that loads data from the backend uses a three-way conditional so the user never sees a red error alert *and* a "no items found" empty-state message at the same time:

```jinja
{% if error %}
  <p class="text-secondary">Couldn't load this section.</p>
{% elif data %}
  ...
{% else %}
  <p class="text-secondary">No items yet.</p>
{% endif %}
```

### Flash alerts

Rendered by `templates/_flash.html` as dismissible Bootstrap alerts with category icons. `success` and `info` auto-dismiss after 5 s (10-line JS snippet in `base.html`); `warning` and `danger` persist until the user closes them.

## Configuration

All config is read from environment variables (or a `.env` file). Key settings:

| Variable | Default | Description |
|---|---|---|
| `BACKEND_API_URL` | `http://localhost:8000/api/v1` | FastAPI base URL |
| `BACKEND_TIMEOUT_S` | `60.0` | HTTP request timeout in seconds |
| `SECRET_KEY` | `dev-secret` | Flask session secret — change in production |
| `APP_ENV` | `development` | Set to `production` to enable production mode |
| `LOG_LEVEL` | `INFO` | Logger level for both Flask and `backend_client` (`DEBUG`/`INFO`/`WARNING`/`ERROR`) |
| `MAX_UPLOAD_SIZE_MB` | `10` | Per-file upload size cap |
| `DEFAULT_CORPUS_ID` | `00000000-0000-0000-0000-000000000001` | Single-workspace MVP corpus ID |

`MAX_CONTENT_LENGTH` (the raw-request-body cap that triggers a 413) is derived as `MAX_UPLOAD_SIZE_MB × 10 × 1024 × 1024` — about 100 MB by default — so Werkzeug rejects oversized payloads before fully buffering them.

## Running with Docker (recommended)

The full stack — database, backend API, and frontend — is defined in `docker-compose.yml` at the repository root. Run everything from the repo root:

```bash
# Build and start all services
docker compose up --build

# Rebuild only the frontend (e.g. after dependency changes)
docker compose build --no-cache frontend
docker compose up -d --no-deps frontend
```

Services started:

| Service | URL |
|---|---|
| Frontend (Flask) | http://localhost:3000 |
| Backend (FastAPI) | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |

## Cache cleanup — if the UI looks stale

After pulling new commits or rebuilding, you may see old CSS / JS / templates. Walk down this table until the page refreshes — try cheaper fixes first.

| # | Layer | Symptom | Fix |
|---|---|---|---|
| 1 | **Browser cache** | New CSS/JS not visible even after a Docker rebuild | Hard refresh: `Ctrl + Shift + R` (or open in Incognito) |
| 2 | **Frontend image** | Container still serves old templates / static files | `docker compose build --no-cache frontend && docker compose up -d --no-deps frontend` |
| 3 | **Docker BuildKit cache** | `--no-cache` rebuild still produces wrong output (most common on Windows + OneDrive) | `docker builder prune -af` then redo step 2 |
| 4 | **Python bytecode** (local dev only) | Tests behave oddly after editing source outside Docker | `Get-ChildItem . -Filter __pycache__ -Recurse -Directory \| Remove-Item -Recurse -Force` |
| 5 | **Full reset** (keeps DB volume) | Nothing above worked | `docker compose down; docker builder prune -af; docker compose build --no-cache; docker compose up -d` |

The nuclear option (`docker system prune -af --volumes`) also drops the postgres data volume, so you lose seeded transcripts and codebooks — use only when you genuinely want a blank database too.

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

The test suite (36 tests) covers:
- Smoke tests for all routes (home, health, transcripts, upload, codebooks, analysis)
- Codebook list — data rendered, empty state, generic + typed backend errors
- Theme browser — frequency data, name from query param, empty state, generic + `BackendNotFoundError` paths
- Ingestion — upload flow, file size validation, transcript listing, generic + `BackendUnavailableError` + `BackendValidationError` paths
- BackendClient — unit tests using `httpx.MockTransport` for each exception category (connect refused, timeout, 404, 422-with-detail-parsing, 5xx, malformed JSON)
- Flask handlers — 404 renders branded page, 413 returns 303 + flash, generic `Exception` renders branded 500 without leaking traceback

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
