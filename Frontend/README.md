# Frontend

Flask + Jinja2 server-rendered frontend. Talks to the FastAPI backend over internal REST/JSON.

## Layout

```
Frontend/
├── web/
│   ├── __init__.py          # Flask app factory
│   ├── config.py            
│   └── controllers/
│       ├── main.py          # /, /health
│       └── ingestion.py     # /transcripts, /transcripts/upload  (stubs)
├── templates/
│   ├── base.html
│   ├── index.html
│   └── ingestion/
│       ├── upload.html
│       └── list.html
├── static/css/main.css
├── tests/
│   ├── conftest.py
│   └── test_smoke.py
├── pyproject.toml
└── .env.example
```

## Quick start

```bash
cp .env.example .env

# Install with uv (recommended)
uv sync --extra dev

# Or with pip
pip install -e .

flask --app web:create_app run --debug
```

Open http://localhost:3000.

To run the smoke tests:

```bash
pytest
```

## Porting templates from Backend

Backend templates use FastAPI/Starlette's static-URL convention:
`{{ url_for('static', path='foo.css') }}`. Flask uses `filename=` instead:
`{{ url_for('static', filename='foo.css') }}`. When copying a template across,
swap the kwarg — otherwise the link silently breaks. Block names (`title`,
`head_extra`, `content`, `scripts`) are aligned, so no other changes needed.
