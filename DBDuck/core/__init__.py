"""Core abstractions for DBDuck."""

from .adapter_router import AdapterRouter
from .exceptions import ConnectionError, DatabaseError, QueryError, TransactionError
from .schema import SchemaValidator

__all__ = [
    "AdapterRouter",
    "SchemaValidator",
    "DatabaseError",
    "ConnectionError",
    "QueryError",
    "TransactionError",
]
