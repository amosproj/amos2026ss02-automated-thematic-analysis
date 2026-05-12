from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    # ---- Runtime ----
    APP_ENV: str = "development"
    APP_DEBUG: bool = False
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 3000  # 5000 collides with macOS AirPlay; 3000 is in backend CORS allowlist
    LOG_LEVEL: str = "INFO"

    # ---- Flask ----
    SECRET_KEY: str = "dev-secret"

    # ---- Backend integration ----
    BACKEND_API_URL: str = "http://localhost:8000/api/v1" 
    BACKEND_TIMEOUT_S: float = 60.0

    # ---- Workspace (single-corpus MVP; UI selector when Projects exist) ----
    DEFAULT_PROJECT_ID: str = "00000000-0000-0000-0000-000000000001"
    DEFAULT_CORPUS_NAME: str = "Interview Transcripts"

    MAX_UPLOAD_SIZE_MB: int = 10
    ACCEPTED_EXTENSIONS: frozenset[str] = frozenset({".txt", ".docx", ".pdf", ".jsonl"})

    @property
    def MAX_UPLOAD_BYTES(self) -> int:
        return self.MAX_UPLOAD_SIZE_MB * 1024 * 1024

    # Werkzeug aborts body parsing once this is exceeded.
    @property
    def MAX_CONTENT_LENGTH(self) -> int:
        return 10 * self.MAX_UPLOAD_BYTES

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"


@lru_cache
def get_config() -> Config:
    """Singleton accessor — matches the backend's get_settings()."""
    return Config()
