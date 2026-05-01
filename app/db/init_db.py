from __future__ import annotations

from loguru import logger

from app.db.base import Base
from app.db.session import engine


def init_db() -> None:
    """
    Initialize database tables.

    Safe to call multiple times; uses SQLAlchemy metadata create_all().
    """
    # Ensure models are imported so they register with Base.metadata
    from app import models  # noqa: F401

    logger.info("db init | creating tables (if missing)")
    Base.metadata.create_all(bind=engine)
    logger.info("db init | done")

