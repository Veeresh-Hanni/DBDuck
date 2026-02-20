"""DBDuck exception hierarchy."""

from __future__ import annotations


class DatabaseError(Exception):
    """Base exception for all DBDuck database errors."""


class ConnectionError(DatabaseError):
    """Raised when a database connection cannot be created or used."""


class QueryError(DatabaseError):
    """Raised when a query fails validation, compilation, or execution."""


class TransactionError(DatabaseError):
    """Raised when transaction handling fails."""
