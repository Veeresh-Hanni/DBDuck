"""Adapter routing for UDOM backends."""

from __future__ import annotations

from urllib.parse import urlparse

from ..adapters.mysql_adapter import MySQLAdapter
from ..adapters.postgres_adapter import PostgresAdapter
from ..adapters.sqlite_adapter import SQLiteAdapter
from .exceptions import ConnectionError


class AdapterRouter:
    """Resolve adapter class based on db_type, db_instance, and URL."""

    _sql_map = {
        "sqlite": SQLiteAdapter,
        "mysql": MySQLAdapter,
        "postgres": PostgresAdapter,
        "postgresql": PostgresAdapter,
    }

    @classmethod
    def infer_sql_instance_from_url(cls, url: str | None) -> str | None:
        if not url:
            return None
        scheme = urlparse(url).scheme.lower()
        if not scheme:
            return None
        dialect = scheme.split("+", 1)[0]
        return "postgres" if dialect == "postgresql" else dialect

    @classmethod
    def route_sql_adapter(cls, db_instance: str | None, url: str | None):
        resolved = (db_instance or "").lower().strip() or cls.infer_sql_instance_from_url(url)
        if not resolved:
            resolved = "sqlite"
        adapter_cls = cls._sql_map.get(resolved)
        if adapter_cls is None:
            raise ConnectionError(f"Unsupported SQL db_instance/url dialect: {db_instance or url}")
        return resolved, adapter_cls

