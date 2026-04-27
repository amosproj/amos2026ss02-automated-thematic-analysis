"""
Service layer — business logic lives here.

Each domain module gets its own service file, e.g.:
    app/services/corpus.py
    app/services/analysis.py
    app/services/codebook.py

Services receive an AsyncSession via dependency injection and
call repository/query helpers. They must not import FastAPI
concerns (Request, Response, status codes).
"""
