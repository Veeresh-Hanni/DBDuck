"""Structured logger helpers."""

from __future__ import annotations

import logging
import os
from typing import Any

_LOGGER_NAME = "DBDuck"
_DEFAULT_FORMAT = (
    "%(asctime)s %(levelname)s %(name)s %(message)s "
    "event=%(event)s db=%(db)s entity=%(entity)s"
)


class _SafeExtraFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "event"):
            record.event = "-"
        if not hasattr(record, "db"):
            record.db = "-"
        if not hasattr(record, "entity"):
            record.entity = "-"
        return super().format(record)


def get_logger(level: str | int | None = None) -> logging.Logger:
    """Return configured project logger."""
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_SafeExtraFormatter(_DEFAULT_FORMAT))
        logger.addHandler(handler)
    resolved = level or os.getenv("DBDUCK_LOG_LEVEL", "INFO")
    logger.setLevel(resolved)
    logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    level: int,
    message: str,
    *,
    event: str,
    db: str = "-",
    entity: str = "-",
    **fields: Any,
) -> None:
    """Emit a structured log event with standard keys."""
    logger.log(level, message, extra={"event": event, "db": db, "entity": entity, **fields})
