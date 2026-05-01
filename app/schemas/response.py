from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class ErrorPayload(BaseModel):
    message: str = Field(..., description="Human-readable error message")
    code: Optional[str] = Field(None, description="Stable application error code")
    details: Optional[Any] = Field(None, description="Extra error details (safe to expose)")


class ApiResponse(BaseModel):
    status: Literal["success", "error"]
    data: Optional[Any] = None
    error: Optional[ErrorPayload] = None

    @classmethod
    def ok(cls, data: Any) -> "ApiResponse":
        return cls(status="success", data=data, error=None)

    @classmethod
    def fail(
        cls,
        *,
        message: str,
        code: str | None = None,
        details: Any | None = None,
    ) -> "ApiResponse":
        return cls(
            status="error",
            data=None,
            error=ErrorPayload(message=message, code=code, details=details),
        )

