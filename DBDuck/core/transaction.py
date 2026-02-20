"""Transaction lifecycle utilities."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy.engine import Connection

from .exceptions import TransactionError


class TransactionManager:
    """Manage one thread-local SQLAlchemy transaction per adapter."""

    def __init__(self, engine) -> None:
        self._engine = engine
        self._local = threading.local()

    def begin(self) -> Connection:
        """Start and hold a transaction-bound connection."""
        if getattr(self._local, "connection", None) is not None:
            raise TransactionError("Transaction already active on this thread")
        conn = self._engine.connect()
        tx = conn.begin()
        self._local.connection = conn
        self._local.transaction = tx
        return conn

    def commit(self) -> None:
        """Commit active transaction."""
        tx = getattr(self._local, "transaction", None)
        conn = getattr(self._local, "connection", None)
        if tx is None or conn is None:
            raise TransactionError("No active transaction to commit")
        try:
            tx.commit()
        except Exception as exc:
            raise TransactionError("Commit failed") from exc
        finally:
            conn.close()
            self._local.connection = None
            self._local.transaction = None

    def rollback(self) -> None:
        """Rollback active transaction."""
        tx = getattr(self._local, "transaction", None)
        conn = getattr(self._local, "connection", None)
        if tx is None or conn is None:
            raise TransactionError("No active transaction to rollback")
        try:
            tx.rollback()
        except Exception as exc:
            raise TransactionError("Rollback failed") from exc
        finally:
            conn.close()
            self._local.connection = None
            self._local.transaction = None

    def get_connection(self) -> Connection | None:
        """Return current thread-local transactional connection if present."""
        return getattr(self._local, "connection", None)

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Context manager for begin/commit/rollback flow."""
        conn = self.begin()
        try:
            yield conn
            self.commit()
        except Exception:
            self.rollback()
            raise
