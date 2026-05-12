import pytest

from web import create_app


@pytest.fixture
def app():
    """Fresh Flask app per test, using config defaults."""
    return create_app()


@pytest.fixture
def client(app):
    return app.test_client()
