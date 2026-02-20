"""Shared SQLAlchemy-backed adapter implementation."""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ..core.base_adapter import BaseAdapter
from ..core.connection_manager import ConnectionManager
from ..core.exceptions import QueryError
from ..core.transaction import TransactionManager
from ..utils.logger import get_logger, log_event


class SQLAlchemyAdapter(BaseAdapter):
    """Reusable SQL adapter with prepared statements and table auto-creation."""

    DIALECT = "sql"
    IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(self, url: str, **options: Any) -> None:
        self.url = url
        self.options = options
        self._logger = get_logger(options.get("log_level"))
        self._conn_manager = ConnectionManager()
        self.engine = self._conn_manager.get_engine(
            url=url,
            pool_size=int(options.get("pool_size", 5)),
            max_overflow=int(options.get("max_overflow", 10)),
            pool_pre_ping=bool(options.get("pool_pre_ping", True)),
            echo=bool(options.get("echo", False)),
        )
        self._tx = TransactionManager(self.engine)
        self._prepared_cache: dict[str, str] = {}
        # Placeholder for future opt-in read cache (e.g. LRU/TTL/Redis-backed).
        self._query_cache: dict[str, Any] = {}

    def _quote(self, name: str) -> str:
        raise NotImplementedError

    def _type_for_value(self, value: Any) -> str:
        raise NotImplementedError

    def _pk_column_sql(self) -> str:
        raise NotImplementedError

    @classmethod
    def _validate_identifier(cls, name: str) -> str:
        if not isinstance(name, str) or not cls.IDENTIFIER_RE.fullmatch(name):
            raise QueryError(f"Invalid SQL identifier: {name!r}")
        return name

    def _validate_data(self, data: Mapping[str, Any]) -> None:
        if not isinstance(data, Mapping) or not data:
            raise QueryError("create() requires a non-empty mapping payload")
        for key in data:
            self._validate_identifier(key)

    def _ensure_table(self, entity: str, data: Mapping[str, Any]) -> None:
        quoted_table = self._quote(entity)
        cols = [self._pk_column_sql()]
        for key, value in data.items():
            cols.append(f"{self._quote(key)} {self._type_for_value(value)}")
        sql = f"CREATE TABLE IF NOT EXISTS {quoted_table} ({', '.join(cols)})"
        self.run_native(sql)

    def _active_connection(self):
        return self._tx.get_connection()

    def _execute(self, sql: str, params: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None):
        conn = self._active_connection()
        if conn is not None:
            return conn.execute(text(sql), params or {})
        with self.engine.begin() as auto_conn:
            return auto_conn.execute(text(sql), params or {})

    def run_native(
        self, query: str, params: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None
    ) -> Any:
        if not isinstance(query, str) or not query.strip():
            raise QueryError("Query must be a non-empty string")
        try:
            log_event(self._logger, 20, "Executing native SQL", event="query.execute", db=self.DIALECT)
            result = self._execute(query, params=params)
            if result.returns_rows:
                rows = result.mappings().all()
                return [dict(row) for row in rows]
            return {"rows_affected": int(result.rowcount or 0)}
        except SQLAlchemyError as exc:
            log_event(self._logger, 40, "Query failed", event="query.error", db=self.DIALECT)
            raise QueryError(str(exc)) from exc

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        entity = self._validate_identifier(entity)
        self._validate_data(data)
        self._ensure_table(entity, data)
        cols = list(data.keys())
        key = f"insert:{entity}:{','.join(cols)}"
        sql = self._prepared_cache.get(key)
        if sql is None:
            col_sql = ", ".join(self._quote(c) for c in cols)
            val_sql = ", ".join(f":{c}" for c in cols)
            sql = f"INSERT INTO {self._quote(entity)} ({col_sql}) VALUES ({val_sql})"
            self._prepared_cache[key] = sql
        return self.run_native(sql, params=data)

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        entity = self._validate_identifier(entity)
        if not isinstance(rows, list) or not rows:
            raise QueryError("create_many() requires a non-empty list of mapping rows")
        for row in rows:
            self._validate_data(row)
        first = rows[0]
        cols = list(first.keys())
        for row in rows:
            if list(row.keys()) != cols:
                raise QueryError("All rows in create_many() must have same field order")
        self._ensure_table(entity, first)
        key = f"insert_many:{entity}:{','.join(cols)}"
        sql = self._prepared_cache.get(key)
        if sql is None:
            col_sql = ", ".join(self._quote(c) for c in cols)
            val_sql = ", ".join(f":{c}" for c in cols)
            sql = f"INSERT INTO {self._quote(entity)} ({col_sql}) VALUES ({val_sql})"
            self._prepared_cache[key] = sql
        return self.run_native(sql, params=rows)

    def _build_where_clause(
        self, where: Mapping[str, Any] | str | None
    ) -> tuple[str, dict[str, Any]]:
        if where is None:
            return "", {}
        if isinstance(where, str):
            # Legacy support path. Callers are responsible for trusted expressions.
            return f" WHERE {where}", {}
        if not isinstance(where, Mapping):
            raise QueryError("where must be a mapping, string, or None")
        parts = []
        params: dict[str, Any] = {}
        for idx, (key, value) in enumerate(where.items()):
            self._validate_identifier(key)
            p = f"w_{idx}"
            parts.append(f"{self._quote(key)} = :{p}")
            params[p] = value
        if not parts:
            return "", {}
        return " WHERE " + " AND ".join(parts), params

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        entity = self._validate_identifier(entity)
        where_sql, params = self._build_where_clause(where)
        sql = f"SELECT * FROM {self._quote(entity)}{where_sql}"
        if order_by:
            safe_order = order_by.strip()
            match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)(?:\s+(ASC|DESC))?", safe_order, re.IGNORECASE)
            if not match:
                raise QueryError("Invalid order_by clause")
            field, direction = match.group(1), (match.group(2) or "ASC").upper()
            self._validate_identifier(field)
            sql += f" ORDER BY {self._quote(field)} {direction}"
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise QueryError("limit must be a positive integer")
            sql += " LIMIT :limit_value"
            params["limit_value"] = limit
        return self.run_native(sql, params=params)

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        entity = self._validate_identifier(entity)
        where_sql, params = self._build_where_clause(where)
        if not where_sql:
            raise QueryError("delete() requires a non-empty where condition")
        sql = f"DELETE FROM {self._quote(entity)}{where_sql}"
        return self.run_native(sql, params=params)

    def convert_uql(self, uql_query: str) -> str:
        uql = uql_query.strip()
        upper = uql.upper()
        if upper.startswith("FIND "):
            match = re.match(
                r"FIND\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?$",
                uql,
                flags=re.IGNORECASE,
            )
            if not match:
                raise QueryError("Invalid FIND UQL")
            entity = match.group(1)
            where = match.group(2)
            order_by = match.group(3)
            limit = int(match.group(4)) if match.group(4) else None
            sql = f"SELECT * FROM {self._quote(self._validate_identifier(entity))}"
            if where:
                sql += f" WHERE {where}"
            if order_by:
                sql += f" ORDER BY {order_by}"
            if limit is not None:
                sql += f" LIMIT {limit}"
            return sql
        if upper.startswith("DELETE "):
            match = re.match(
                r"DELETE\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+WHERE\s+(.+))?$",
                uql,
                flags=re.IGNORECASE,
            )
            if not match:
                raise QueryError("Invalid DELETE UQL")
            entity = self._validate_identifier(match.group(1))
            where = match.group(2)
            if not where:
                raise QueryError("DELETE UQL requires WHERE")
            return f"DELETE FROM {self._quote(entity)} WHERE {where}"
        if upper.startswith("CREATE "):
            match = re.match(r"CREATE\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{(.+)\}$", uql, flags=re.IGNORECASE)
            if not match:
                raise QueryError("Invalid CREATE UQL")
            entity = self._validate_identifier(match.group(1))
            body = match.group(2)
            pairs = [p.strip() for p in body.split(",") if p.strip()]
            cols: list[str] = []
            vals: list[str] = []
            for pair in pairs:
                if ":" not in pair:
                    raise QueryError("Invalid CREATE UQL payload")
                key, raw = pair.split(":", 1)
                key = self._validate_identifier(key.strip())
                cols.append(self._quote(key))
                vals.append(raw.strip())
            self._ensure_table(entity, {c.strip('"`[]'): "" for c in cols})
            return f"INSERT INTO {self._quote(entity)} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
        raise QueryError("Unsupported UQL command")

    def begin(self):
        return self._tx.begin()

    def commit(self) -> None:
        self._tx.commit()

    def rollback(self) -> None:
        self._tx.rollback()

    def transaction(self):
        return self._tx.transaction()
