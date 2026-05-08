"""Lightweight logging wrapper.

Uses :class:`rich.logging.RichHandler` when available, falls back to plain
``logging.StreamHandler`` otherwise. Noisy third-party loggers (httpx, urllib3,
asyncio) are forced to WARNING in one place so callers do not have to remember
``setLevel`` boilerplate.
"""

from __future__ import annotations

import logging
import os
from functools import cache

_DEFAULT_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"

_NOISY_LIBS = (
    "httpx",
    "httpcore",
    "urllib3",
    "asyncio",
    "kaleido",
    "choreographer",
    "logistro",
)


@cache
def _configure_root() -> None:
    level_name = os.getenv("GEOGRAPHY_VN_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler: logging.Handler
    try:
        from rich.logging import RichHandler

        handler = RichHandler(rich_tracebacks=True, show_time=True, show_path=False)
        fmt = "%(message)s"
    except Exception:
        handler = logging.StreamHandler()
        fmt = _DEFAULT_FORMAT

    handler.setFormatter(logging.Formatter(fmt))
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]

    for _lib in _NOISY_LIBS:
        logging.getLogger(_lib).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    _configure_root()
    return logging.getLogger(name)
