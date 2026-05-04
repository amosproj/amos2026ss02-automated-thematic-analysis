from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from loguru import logger

from app.schemas.common import ResponseEnvelope


class AppException(Exception):
    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    message: str = "An unexpected error occurred"

    def __init__(self, message: str | None = None) -> None:
        self.message = message or self.__class__.message
        super().__init__(self.message)


class NotFoundError(AppException):
    status_code = status.HTTP_404_NOT_FOUND
    message = "Resource not found"


class ConflictError(AppException):
    status_code = status.HTTP_409_CONFLICT
    message = "Resource already exists"


class UnprocessableError(AppException):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    message = "Validation failed"


class UnauthorizedError(AppException):
    status_code = status.HTTP_401_UNAUTHORIZED
    message = "Authentication required"


class ForbiddenError(AppException):
    status_code = status.HTTP_403_FORBIDDEN
    message = "Permission denied"


def _error_response(status_code: int, error: str, detail: str | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=ResponseEnvelope.fail(error, detail).model_dump(),
    )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppException)
    async def app_exception_handler(request: Request, exc: AppException) -> JSONResponse:
        return _error_response(exc.status_code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = "; ".join(
            f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}"
            for e in exc.errors()
        )
        return _error_response(status.HTTP_422_UNPROCESSABLE_ENTITY, "Validation failed", errors)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception on {} {}", request.method, request.url.path)
        return _error_response(status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal server error")
