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

    MAX_UPLOAD_BYTES: int = 10 * 1024 * 1024 # 10 MB limit for uploaded interview files
    UPLOADS_DIR: str = "uploads"
    DEMOGRAPHIC_UPLOAD_TTL_SECONDS: int = 60 * 60
    UPLOAD_CLEANUP_INTERVAL_SECONDS: int = 5 * 60

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production" # Helper property to check if the app is running in production mode

    @field_validator("CORS_ALLOWED_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v


    LLM_PROVIDER: str = "academic_cloud"  # "academic_cloud" | "litellm"
    LLM_BASE_URL: str = "https://chat-ai.academiccloud.de/v1"
    LLM_API_KEY: str | None = None
    LLM_MODEL: str = "gemma-4-31b-it"  # "mistral-large-3-675b-instruct-2512" or qwen variants
    LLM_TEMPERATURE: float = 0.2
    LLM_REQUEST_TIMEOUT_S: float = 120.0  # too generous for the current test with a single interview file

    # NHR@FAU LLM gateway (https://hpc.fau.de/request-llm-api-key/)
    LLM_API_KEY_FAU: str | None = None
    LLM_BASE_URL_FAU: str = "https://hub.nhr.fau.de/api/llmgw/v1"
    LLM_MODEL_FAU: str = "gpt-oss-120b"

    # Select which provider is active: "FAU" | "ACADEMIC"
    SELECTED_API: str = "FAU"


@lru_cache
def get_settings() -> Settings:
    return Settings()
