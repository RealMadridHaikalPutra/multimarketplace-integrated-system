"""Application-wide logging configuration."""

from __future__ import annotations

import logging
import sys

from core.config import settings


def configure_logging() -> None:
    """Configure root logging once at application startup."""
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    formatter = logging.Formatter(settings.log_format)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    # Remove default handlers to avoid duplicates on reload.
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # Reduce third-party noise.
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
