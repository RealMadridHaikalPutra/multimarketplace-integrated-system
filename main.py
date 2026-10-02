"""Compatibility ASGI entrypoint for ``uvicorn main:app``."""

from app.main import app

__all__ = ["app"]
