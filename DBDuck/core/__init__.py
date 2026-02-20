"""Core abstractions for DBDuck."""

from .exceptions import ConnectionError, DatabaseError, QueryError, TransactionError

__all__ = ["DatabaseError", "ConnectionError", "QueryError", "TransactionError"]
