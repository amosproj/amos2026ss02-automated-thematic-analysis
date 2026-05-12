from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    # ---- Runtime params  ----
    APP_ENV: str = "development"
    APP_DEBUG: bool = False
    APP_HOST: str = "0.0.0.0"
    # Port 3000 (not Flask's 5000 default), avoids the macOS AirPlay Receiver - can be restored after docker
    # clash and matches the backend's CORS_ALLOWED_ORIGINS default.
    APP_PORT: int = 3000
    LOG_LEVEL: str = "INFO"

    # ---- Flask ----
    SECRET_KEY: str = "dev-secret"

    # ---- Backend integration ----
    # In Docker compose this is overridden to `http://api:8000/api/v1`
    BACKEND_API_URL: str = "http://localhost:8000/api/v1"

    MAX_UPLOAD_SIZE_MB: int = 10

    ACCEPTED_EXTENSIONS: frozenset[str] = frozenset({".txt", ".docx", ".pdf", ".jsonl"})

    @property
    def MAX_CONTENT_LENGTH(self) -> int:
        """Flask reads this from app.config — request body size cap in bytes."""
        return self.MAX_UPLOAD_SIZE_MB * 1024 * 1024

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"


@lru_cache
def get_config() -> Config:
    """Singleton accessor. Construction reads .env and validates types, so we
    cache the instance per process. Matches the backend's get_settings()."""
    return Config()
