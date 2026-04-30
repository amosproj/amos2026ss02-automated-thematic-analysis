from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    APP_ENV: str = "development"
    APP_DEBUG: bool = False
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    DATABASE_URL: str

    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"

    CORS_ALLOWED_ORIGINS: list[str] = ["http://localhost:3000"] # Default allowed origin for development
    API_V1_PREFIX: str = "/api/v1"  # Default API prefix

    INGESTION_CHUNK_SIZE_WORDS: int = 2048
    INGESTION_CHUNK_OVERLAP_WORDS: int = 200
    INGESTION_MAX_DOCUMENT_WORDS: int = 100_000
    INGESTION_DEDUPLICATE_BY_HASH: bool = True

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production" # Helper property to check if the app is running in production mode

    @field_validator("CORS_ALLOWED_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: str | list) -> list[str]:
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()
