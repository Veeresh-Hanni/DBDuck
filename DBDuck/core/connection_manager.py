"""Thread-safe SQLAlchemy engine/session manager."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import scoped_session, sessionmaker

from .exceptions import ConnectionError
from ..utils.logger import get_logger, log_event


@dataclass(frozen=True)
class ParsedDatabaseURL:
    """Normalized database URL details."""

    raw_url: str
    dialect: str
    host: str | None
    port: int | None
    database: str | None


class ConnectionManager:
    """Lazily creates and reuses engines/sessions per URL."""

    _instance: "ConnectionManager | None" = None
    _instance_lock = threading.Lock()

    def __new__(cls) -> "ConnectionManager":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._engines = {}
                    cls._instance._sessions = {}
                    cls._instance._lock = threading.RLock()
                    cls._instance._logger = get_logger()
        return cls._instance

    @staticmethod
    def parse_url(url: str) -> ParsedDatabaseURL:
        """Parse and normalize SQLAlchemy-style database URL."""
        if not isinstance(url, str) or not url.strip():
            raise ConnectionError("Database URL must be a non-empty string")
        parsed = urlparse(url)
        if not parsed.scheme:
            raise ConnectionError(f"Invalid database URL: {url!r}")
        dialect = parsed.scheme.split("+", 1)[0].lower()
        database = parsed.path.lstrip("/") if parsed.path else None
        return ParsedDatabaseURL(
            raw_url=url,
            dialect=dialect,
            host=parsed.hostname,
            port=parsed.port,
            database=database or None,
        )

    def get_engine(
        self,
        url: str,
        *,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_pre_ping: bool = True,
        echo: bool = False,
    ) -> Engine:
        """Get or build an Engine for the URL with pooling."""
        with self._lock:
            engine = self._engines.get(url)
            if engine is not None:
                return engine
            try:
                parsed = self.parse_url(url)
                kwargs: dict[str, Any] = {"echo": echo}
                if parsed.dialect != "sqlite":
                    kwargs.update(
                        {
                            "pool_size": pool_size,
                            "max_overflow": max_overflow,
                            "pool_pre_ping": pool_pre_ping,
                        }
                    )
                engine = create_engine(url, future=True, **kwargs)
                self._engines[url] = engine
                log_event(
                    self._logger,
                    20,
                    "Database engine created",
                    event="connection.create",
                    db=parsed.dialect,
                )
                return engine
            except Exception as exc:
                raise ConnectionError(f"Failed to create engine for URL {url!r}") from exc

    def get_scoped_session(self, url: str):
        """Get scoped_session bound to the URL engine."""
        with self._lock:
            session = self._sessions.get(url)
            if session is not None:
                return session
            engine = self.get_engine(url)
            factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
            session = scoped_session(factory)
            self._sessions[url] = session
            return session
