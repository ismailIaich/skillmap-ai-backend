from __future__ import annotations

import sys
import time
from typing import Callable

from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<level>{message}</level>"
)


def configure_logging(level: str = "INFO") -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        level=level.upper(),
        format=LOG_FORMAT,
        backtrace=False,
        diagnose=False,
        enqueue=True,
    )


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, *, should_log: Callable[[Request], bool] | None = None):
        super().__init__(app)
        self._should_log = should_log or (lambda _req: True)

    async def dispatch(self, request: Request, call_next) -> Response:
        if not self._should_log(request):
            return await call_next(request)

        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.exception(
                "request failed | {method} {path} | {elapsed_ms:.2f}ms",
                method=request.method,
                path=request.url.path,
                elapsed_ms=elapsed_ms,
            )
            raise

        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            "{method} {path} -> {status_code} | {elapsed_ms:.2f}ms",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            elapsed_ms=elapsed_ms,
        )
        return response

