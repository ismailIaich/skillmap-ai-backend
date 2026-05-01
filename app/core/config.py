from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field


load_dotenv()


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(f"{name} must be set")
    return value


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


class Settings(BaseModel):
    DATABASE_URL: str = Field(..., description="PostgreSQL connection string")
    TAVILY_API_KEY: str = Field(..., description="Tavily API key")
    HF_API_KEY: str = Field(..., description="HuggingFace API key")

    # Optional infra toggles (safe defaults)
    LOG_LEVEL: str = Field("INFO", description="Application log level")
    SQLALCHEMY_ECHO: bool = Field(False, description="Enable SQLAlchemy engine echo")

    @classmethod
    def from_env(cls) -> "Settings":
        data: dict[str, Any] = {
            "DATABASE_URL": _require_env("DATABASE_URL"),
            "TAVILY_API_KEY": _require_env("TAVILY_API_KEY"),
            "HF_API_KEY": _require_env("HF_API_KEY"),
            "LOG_LEVEL": (os.getenv("LOG_LEVEL") or "INFO").strip() or "INFO",
            "SQLALCHEMY_ECHO": _parse_bool(os.getenv("SQLALCHEMY_ECHO"), default=False),
        }
        return cls(**data)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()

