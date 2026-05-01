from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from starlette.responses import JSONResponse

from app.api.v1.router import api_router
from app.core.config import get_settings
from app.core.logging import RequestLoggingMiddleware, configure_logging
from app.schemas.response import ApiResponse


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.LOG_LEVEL)

    app = FastAPI(title="SkillMap AI Backend", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(RequestLoggingMiddleware)

    app.include_router(api_router)

    @app.on_event("startup")
    def on_startup() -> None:
        logger.info("app startup | SkillMap AI Backend")

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        logger.warning(
            "validation error | {method} {path}",
            method=request.method,
            path=request.url.path,
        )
        return JSONResponse(
            status_code=422,
            content=ApiResponse.fail(
                message="Validation error",
                code="VALIDATION_ERROR",
                details=exc.errors(),
            ).dict(),
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        logger.warning(
            "http error | {status_code} | {method} {path} | {detail}",
            status_code=exc.status_code,
            method=request.method,
            path=request.url.path,
            detail=exc.detail,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=ApiResponse.fail(
                message=str(exc.detail),
                code="HTTP_ERROR",
            ).dict(),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        logger.exception(
            "unhandled error | {method} {path}",
            method=request.method,
            path=request.url.path,
        )
        return JSONResponse(
            status_code=500,
            content=ApiResponse.fail(
                message="Internal server error",
                code="INTERNAL_SERVER_ERROR",
                details=_safe_error_details(exc),
            ).dict(),
        )

    return app


def _safe_error_details(exc: Exception) -> dict[str, Any]:
    # Keep this intentionally minimal to avoid leaking secrets.
    return {"type": exc.__class__.__name__}


app = create_app()

