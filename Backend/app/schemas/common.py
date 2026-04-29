from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, field_validator

T = TypeVar("T")


class BaseSchema(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        str_strip_whitespace=True,
    )


class ResponseEnvelope(BaseSchema, Generic[T]):
    success: bool
    data: T | None = None
    error: str | None = None
    meta: dict[str, Any] | None = None

    @classmethod
    def ok(cls, data: T, meta: dict[str, Any] | None = None) -> "ResponseEnvelope[T]":
        return cls(success=True, data=data, meta=meta)

    @classmethod
    def fail(cls, error: str, detail: str | None = None) -> "ResponseEnvelope[None]":
        return cls(
            success=False,
            error=error,
            meta={"detail": detail} if detail else None,
        )


class PageMeta(BaseSchema):
    total: int
    page: int
    page_size: int
    pages: int


class Page(BaseSchema, Generic[T]):
    items: list[T]
    meta: PageMeta


class PaginationParams(BaseSchema):
    page: int = 1
    page_size: int = 20

    @field_validator("page")
    @classmethod
    def page_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("page must be >= 1")
        return v

    @field_validator("page_size")
    @classmethod
    def page_size_bounded(cls, v: int) -> int:
        if not 1 <= v <= 100:
            raise ValueError("page_size must be between 1 and 100")
        return v


class HealthResponse(BaseSchema):
    status: Literal["ok", "degraded"]
    database: Literal["up", "down"]
    version: str
