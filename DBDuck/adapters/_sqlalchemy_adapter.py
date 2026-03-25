"""Shared SQLAlchemy-backed adapter implementation."""

from __future__ import annotations

import builtins
import re
from typing import Any, Mapping, Sequence

from sqlalchemy import MetaData, Table, and_, bindparam, delete as sa_delete, func, insert, inspect as sa_inspect, select, text, update as sa_update
from sqlalchemy.exc import DisconnectionError, InterfaceError, OperationalError, SQLAlchemyError

from ..core.base_adapter import BaseAdapter
from ..core.connection_manager import ConnectionManager
from ..core.exceptions import ConnectionError, QueryError
from ..core.transaction import TransactionManager
from ..utils.logger import get_logger, log_event, log_internal_debug


class SQLAlchemyAdapter(BaseAdapter):
    """Reusable SQL adapter with prepared statements and table auto-creation."""

    DIALECT = "sql"
    IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    _DANGEROUS_SQL = re.compile(
        r"(?:--|#|/\*|\*/|\b(UNION|DROP|TRUNCATE|ALTER|SLEEP|BENCHMARK|EXEC|EVAL|FUNCTION)\b)",
        re.IGNORECASE,
    )
    _DDL_DANGEROUS_SQL = re.compile(r"(?:--|#|/\*|\*/|\b(DROP|TRUNCATE|ALTER)\b)", re.IGNORECASE)
    _SQLALCHEMY_BACKGROUND_RE = re.compile(
        r"\s*\(?\s*Background on this error at:\s*https?://sqlalche\.me/[^\s\)]*\s*\)?\s*",
        re.IGNORECASE,
    )
    _SQLALCHEMY_URL_RE = re.compile(r"\s*https?://sqlalche\.me/[^\s\)]*\s*", re.IGNORECASE)
    _CONNECTION_ERROR_CODES = {2002, 2003, 2006, 2013}
    _AGG_FUNC_RE = re.compile(r"^\s*(count|sum|avg|min|max)\s*\(\s*(\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$", re.IGNORECASE)
    _PUBLIC_QUERY_ERROR = "Database execution failed"
    _PUBLIC_CONNECTION_ERROR = "Database connection failed"

    def __init__(self, url: str, **options: Any) -> None:
        self.url = url
        self.options = options
        self._logger = get_logger(options.get("log_level"))
        self._conn_manager = ConnectionManager()
        self.engine = self._conn_manager.get_engine(
            url=url,
            pool_size=int(options.get("pool_size", 5)),
            max_overflow=int(options.get("max_overflow", 10)),
            pool_timeout=int(options.get("pool_timeout", 30)),
            pool_recycle=int(options.get("pool_recycle", 1800)),
            pool_pre_ping=bool(options.get("pool_pre_ping", True)),
            echo=False,
        )
        self._tx = TransactionManager(self.engine)
        self._prepared_cache: dict[str, str] = {}
        # Placeholder for future opt-in read cache (e.g. LRU/TTL/Redis-backed).
        self._query_cache: dict[str, Any] = {}
        self._column_type_cache: dict[str, dict[str, str]] = {}
        self._metadata = MetaData()
        self._table_cache: dict[str, Table] = {}

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

    def _get_column_type_map(self, entity: str) -> dict[str, str]:
        if not entity:
            return {}
        cached = self._column_type_cache.get(entity)
        if cached is not None:
            return cached
        try:
            columns = sa_inspect(self.engine).get_columns(entity)
        except Exception:
            columns = []
        column_map = {str(col["name"]): str(col["type"]).lower() for col in columns if col.get("name")}
        self._column_type_cache[entity] = column_map
        return column_map

    @staticmethod
    def _is_integer_type(type_name: str) -> bool:
        return any(token in type_name for token in ("int", "integer", "serial"))

    @staticmethod
    def _is_float_type(type_name: str) -> bool:
        return any(token in type_name for token in ("float", "double", "real", "numeric", "decimal"))

    @staticmethod
    def _is_boolean_type(type_name: str) -> bool:
        return any(token in type_name for token in ("bool", "boolean", "bit"))

    def _normalize_value_for_column(self, entity: str, field: str, value: Any) -> Any:
        if value is None:
            return None
        type_name = self._get_column_type_map(entity).get(field, "")
        if not type_name:
            return value
        if self._is_boolean_type(type_name):
            if isinstance(value, bool):
                return value
            if isinstance(value, int) and value in (0, 1):
                return bool(value)
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"true", "1"}:
                    return True
                if lowered in {"false", "0"}:
                    return False
            raise QueryError(f"Invalid boolean value for field '{field}'")
        if self._is_integer_type(type_name):
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
                return int(value.strip())
            raise QueryError(f"Invalid integer value for field '{field}'")
        if self._is_float_type(type_name):
            if isinstance(value, bool):
                raise QueryError(f"Invalid numeric value for field '{field}'")
            if isinstance(value, (int, float)):
                return value
            if isinstance(value, str) and re.fullmatch(r"-?\d+(?:\.\d+)?", value.strip()):
                text_value = value.strip()
                return float(text_value) if "." in text_value else int(text_value)
            raise QueryError(f"Invalid numeric value for field '{field}'")
        return value

    def _ensure_table(self, entity: str, data: Mapping[str, Any]) -> None:
        quoted_table = self._quote(entity)
        has_explicit_id = any(k.lower() == "id" for k in data)
        cols = [] if has_explicit_id else [self._pk_column_sql()]
        for key, value in data.items():
            cols.append(f"{self._quote(key)} {self._type_for_value(value)}")
        sql = f"CREATE TABLE IF NOT EXISTS {quoted_table} ({', '.join(cols)})"
        self.run_native(sql)
        self._column_type_cache.pop(entity, None)
        self._table_cache.pop(entity, None)

    def _get_table(self, entity: str) -> Table:
        cached = self._table_cache.get(entity)
        if cached is not None:
            return cached
        table = Table(entity, self._metadata, autoload_with=self.engine, extend_existing=True)
        self._table_cache[entity] = table
        return table

    def _resolve_column(self, table: Table, field: str):
        self._validate_identifier(field)
        try:
            return table.c[field]
        except KeyError as exc:
            raise QueryError(f"Unknown field '{field}' for entity '{table.name}'") from exc

    def _parse_order_by_components(self, order_by: str) -> tuple[str, str]:
        safe_order = order_by.strip()
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)(?:\s+(ASC|DESC))?", safe_order, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid order_by clause")
        field, direction = match.group(1), (match.group(2) or "ASC").upper()
        self._validate_identifier(field)
        return field, direction

    def _is_trusted_ddl_caller(self) -> bool:
        return bool(self.options.get("admin_mode", False))

    def _require_admin_mode(self, action: str) -> None:
        if not self._is_trusted_ddl_caller():
            raise QueryError(f"{action} requires explicit admin_mode=True")

    @staticmethod
    def _contains_unquoted_semicolon(value: str) -> bool:
        in_single = False
        in_double = False
        escaped = False
        for char in value:
            if escaped:
                escaped = False
                continue
            if char == "\\":
                escaped = True
                continue
            if char == "'" and not in_double:
                in_single = not in_single
                continue
            if char == '"' and not in_single:
                in_double = not in_double
                continue
            if char == ";" and not in_single and not in_double:
                return True
        return False

    def _validate_admin_sql_fragment(self, fragment: str, *, field_name: str) -> str:
        text_fragment = fragment.strip()
        if not text_fragment:
            raise QueryError(f"{field_name} must be a non-empty SQL string")
        if self._contains_unquoted_semicolon(text_fragment):
            raise QueryError(f"{field_name} cannot contain semicolons outside quoted strings")
        if self._DDL_DANGEROUS_SQL.search(text_fragment):
            raise QueryError(f"{field_name} contains prohibited SQL content")
        return text_fragment

    def _parse_uql_create_body(self, body: str) -> dict[str, Any]:
        parsed: dict[str, Any] = {}
        pairs = [p.strip() for p in body.split(",") if p.strip()]
        for pair in pairs:
            if ":" not in pair:
                raise QueryError("Invalid CREATE UQL payload")
            key, raw = pair.split(":", 1)
            field_name = self._validate_identifier(key.strip())
            raw_value = raw.strip()
            starts_quoted = raw_value.startswith(("'", '"'))
            ends_quoted = raw_value.endswith(("'", '"'))
            if starts_quoted != ends_quoted:
                raise QueryError("Invalid quoted literal in CREATE UQL payload")
            parsed[field_name] = self._parse_literal_value(raw_value)
        if not parsed:
            raise QueryError("CREATE UQL requires at least one field")
        return parsed

    def _build_where_expression(
        self,
        entity: str,
        table: Table,
        where: Mapping[str, Any] | str | None,
    ) -> tuple[Any | None, dict[str, Any]]:
        if where is None:
            return None, {}
        if isinstance(where, str):
            where_sql, params = self._build_parameterized_where_from_string(entity, where)
            if not where_sql:
                return None, {}
            return text(where_sql.removeprefix(" WHERE ")), params
        if not isinstance(where, Mapping):
            raise QueryError("where must be a mapping, string, or None")
        conditions = []
        params: dict[str, Any] = {}
        for idx, (key, value) in enumerate(where.items()):
            column = self._resolve_column(table, key)
            pname = f"w_{idx}"
            conditions.append(column == bindparam(pname))
            params[pname] = self._normalize_value_for_column(entity, key, value)
        if not conditions:
            return None, {}
        return and_(*conditions), params

    def _build_having_expression(
        self,
        entity: str,
        table: Table,
        having: Mapping[str, Any] | str | None,
    ) -> tuple[Any | None, dict[str, Any]]:
        return self._build_where_expression(entity, table, having)

    def _build_having_clause(self, entity: str, having: Mapping[str, Any] | str | None) -> tuple[str, dict[str, Any]]:
        where_sql, params = self._build_where_clause(entity, having)
        if not where_sql:
            return "", {}
        return " HAVING " + where_sql.removeprefix(" WHERE "), params

    def _active_connection(self):
        return self._tx.get_connection()

    @staticmethod
    def _consume_result(query: str, result: Any) -> Any:
        if result.returns_rows:
            try:
                rows = result.mappings().all()
                return [dict(row) for row in rows]
            except Exception as fetch_exc:
                # Some DBAPI drivers (notably pyodbc/MSSQL) can misreport row-returning
                # state for control-flow DDL/IF batches. Fall back for non-read statements.
                text_query = query.lstrip().lower()
                if text_query.startswith("select") or text_query.startswith("with"):
                    raise fetch_exc
                return {"rows_affected": int(result.rowcount or 0)}
        return {"rows_affected": int(result.rowcount or 0)}

    def run_native(
        self, query: Any, params: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None
    ) -> Any:
        if isinstance(query, str):
            if not query.strip():
                raise QueryError("Query must be a non-empty string")
            executable = text(query)
            query_text = query
        else:
            executable = query
            query_text = str(query)
        result = None
        try:
            log_event(self._logger, 20, "Executing native SQL", event="query.execute", db=self.DIALECT)
            conn = self._active_connection()
            if conn is not None:
                result = conn.execute(executable, params or {})
                return self._consume_result(query_text, result)
            with self.engine.begin() as auto_conn:
                result = auto_conn.execute(executable, params or {})
                return self._consume_result(query_text, result)
        except SQLAlchemyError as exc:
            log_event(self._logger, 40, "Query failed", event="query.error", db=self.DIALECT)
            log_internal_debug(
                self._logger,
                "Internal SQL execution failure",
                event="query.error.internal",
                db=self.DIALECT,
                exc=exc,
            )
            if self._is_connection_error(exc):
                raise ConnectionError(self._PUBLIC_CONNECTION_ERROR) from exc
            raise QueryError(self._PUBLIC_QUERY_ERROR) from exc
        except Exception as exc:
            log_event(self._logger, 40, "Query failed", event="query.error", db=self.DIALECT)
            log_internal_debug(
                self._logger,
                "Internal SQL execution failure",
                event="query.error.internal",
                db=self.DIALECT,
                exc=exc,
            )
            if self._is_connection_like_exception(exc):
                raise ConnectionError(self._PUBLIC_CONNECTION_ERROR) from exc
            raise QueryError(self._PUBLIC_QUERY_ERROR) from exc
        finally:
            if result is not None:
                try:
                    result.close()
                except Exception as exc:
                    log_internal_debug(
                        self._logger,
                        "Result close failed",
                        event="query.cleanup.internal",
                        db=self.DIALECT,
                        exc=exc,
                    )

    @classmethod
    def _clean_error_message(cls, exc: Exception) -> str:
        message = str(exc).strip()
        cleaned = cls._SQLALCHEMY_BACKGROUND_RE.sub("", message)
        cleaned = cls._SQLALCHEMY_URL_RE.sub("", cleaned)
        cleaned = re.sub(r"\(\s*\)$", "", cleaned).strip()
        return cleaned or "Database query failed"

    @classmethod
    def _is_connection_error(cls, exc: SQLAlchemyError) -> bool:
        if isinstance(exc, (InterfaceError, DisconnectionError)):
            return True
        if isinstance(exc, OperationalError):
            lowered = str(exc).lower()
            markers = (
                "can't connect",
                "connection refused",
                "connection timed out",
                "could not connect",
                "server has gone away",
                "lost connection",
                "connection reset",
                "connection aborted",
                "is the server running",
                "name or service not known",
                "unknown mysql server host",
                "could not translate host name",
                "nodename nor servname provided",
                "unknown database",
                "authentication failed",
                "password authentication failed",
                "login failed",
            )
            if any(token in lowered for token in markers):
                return True
            if (
                "connection to server at" in lowered
                and "database " in lowered
                and "does not exist" in lowered
            ):
                return True
        orig = getattr(exc, "orig", None)
        if isinstance(orig, (OSError, TimeoutError)):
            return True
        args = getattr(orig, "args", ()) if orig is not None else ()
        if args:
            code = args[0]
            if isinstance(code, int) and code in cls._CONNECTION_ERROR_CODES:
                return True
            if isinstance(code, str) and code.isdigit() and int(code) in cls._CONNECTION_ERROR_CODES:
                return True
            if isinstance(code, str):
                normalized = code.strip().upper()
                # SQLSTATE class 08*** is connection exception across SQL dialects.
                if normalized.startswith("08"):
                    return True
                if normalized in {"HYT00", "HYT01"}:
                    return True
        return False

    @classmethod
    def _is_connection_like_exception(cls, exc: Exception) -> bool:
        if isinstance(exc, SQLAlchemyError):
            return cls._is_connection_error(exc)
        if isinstance(exc, (OSError, TimeoutError, builtins.ConnectionError)):
            return True
        lowered = str(exc).lower()
        markers = (
            "can't connect",
            "connection refused",
            "connection timed out",
            "could not connect",
            "server has gone away",
            "lost connection",
            "connection reset",
            "connection aborted",
            "is the server running",
            "name or service not known",
            "unknown mysql server host",
            "could not translate host name",
            "nodename nor servname provided",
        )
        return any(token in lowered for token in markers)

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        entity = self._validate_identifier(entity)
        self._validate_data(data)
        self._ensure_table(entity, data)
        table = self._get_table(entity)
        normalized_data = {key: self._normalize_value_for_column(entity, key, value) for key, value in data.items()}
        stmt = insert(table).values(**normalized_data)
        return self.run_native(stmt)

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
        table = self._get_table(entity)
        normalized_rows = [
            {key: self._normalize_value_for_column(entity, key, value) for key, value in row.items()} for row in rows
        ]
        stmt = insert(table)
        return self.run_native(stmt, params=normalized_rows)

    def _build_where_clause(
        self, entity: str, where: Mapping[str, Any] | str | None
    ) -> tuple[str, dict[str, Any]]:
        if where is None:
            return "", {}
        if isinstance(where, str):
            return self._build_parameterized_where_from_string(entity, where)
        if not isinstance(where, Mapping):
            raise QueryError("where must be a mapping, string, or None")
        parts = []
        params: dict[str, Any] = {}
        for idx, (key, value) in enumerate(where.items()):
            self._validate_identifier(key)
            p = f"w_{idx}"
            parts.append(f"{self._quote(key)} = :{p}")
            params[p] = self._normalize_value_for_column(entity, key, value)
        if not parts:
            return "", {}
        return " WHERE " + " AND ".join(parts), params

    def _build_parameterized_where_from_string(self, entity: str, where: str) -> tuple[str, dict[str, Any]]:
        text_where = where.strip()
        if not text_where:
            return "", {}
        if self._DANGEROUS_SQL.search(text_where):
            raise QueryError("Potential SQL injection detected in where clause")
        tokens = re.split(r"\s+(AND|OR)\s+", text_where, flags=re.IGNORECASE)
        clauses: list[str] = []
        params: dict[str, Any] = {}
        i = 0
        connector_expected = False
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            upper = token.upper()
            if upper in {"AND", "OR"}:
                if not connector_expected:
                    raise QueryError("Invalid where clause structure")
                clauses.append(upper)
                connector_expected = False
                continue
            match = re.fullmatch(
                r"([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*(.+)",
                token,
                flags=re.IGNORECASE,
            )
            if not match:
                raise QueryError("Unsupported where string format; use dict or simple expressions")
            field, op, raw_value = match.group(1), match.group(2), match.group(3).strip()
            self._validate_identifier(field)
            pname = f"ws_{i}"
            i += 1
            value = self._normalize_value_for_column(entity, field, self._parse_literal_value(raw_value))
            clauses.append(f"{self._quote(field)} {op} :{pname}")
            params[pname] = value
            connector_expected = True
        if not clauses:
            return "", {}
        if clauses[-1] in {"AND", "OR"}:
            raise QueryError("Invalid where clause structure")
        return " WHERE " + " ".join(clauses), params

    def _validate_order_by_clause(self, order_by: str) -> str:
        safe_order = order_by.strip()
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)(?:\s+(ASC|DESC))?", safe_order, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid order_by clause")
        field, direction = match.group(1), (match.group(2) or "ASC").upper()
        self._validate_identifier(field)
        return f"{self._quote(field)} {direction}"

    @staticmethod
    def _parse_literal_value(raw: str) -> Any:
        value = raw.strip()
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            return value[1:-1]
        lower = value.lower()
        if lower == "true":
            return True
        if lower == "false":
            return False
        if re.fullmatch(r"-?\d+", value):
            return int(value)
        if re.fullmatch(r"-?\d+\.\d+", value):
            return float(value)
        return value

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._find_with_paging(entity, where=where, order_by=order_by, limit=limit, offset=None)

    def _find_with_paging(
        self,
        entity: str,
        *,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        entity = self._validate_identifier(entity)
        table = self._get_table(entity)
        where_expr, params = self._build_where_expression(entity, table, where)
        stmt = select(table)
        if where_expr is not None:
            stmt = stmt.where(where_expr)
        if order_by:
            field, direction = self._parse_order_by_components(order_by)
            column = self._resolve_column(table, field)
            stmt = stmt.order_by(column.asc() if direction == "ASC" else column.desc())
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise QueryError("limit must be a positive integer")
            stmt = stmt.limit(limit)
        if offset is not None:
            if not isinstance(offset, int) or offset < 0:
                raise QueryError("offset must be a non-negative integer")
            stmt = stmt.offset(offset)
        return self.run_native(stmt, params=params)

    def paginate(
        self,
        entity: str,
        *,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int,
        offset: int,
    ) -> Any:
        return self._find_with_paging(entity, where=where, order_by=order_by, limit=limit, offset=offset)

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        entity = self._validate_identifier(entity)
        table = self._get_table(entity)
        where_expr, params = self._build_where_expression(entity, table, where)
        if where_expr is None:
            raise QueryError("delete() requires a non-empty where condition")
        stmt = sa_delete(table).where(where_expr)
        return self.run_native(stmt, params=params)

    def update(self, entity: str, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        entity = self._validate_identifier(entity)
        self._validate_data(data)
        table = self._get_table(entity)
        where_expr, where_params = self._build_where_expression(entity, table, where)
        if where_expr is None:
            raise QueryError("update() requires a non-empty where condition")
        normalized_data = {}
        for key, value in data.items():
            self._validate_identifier(key)
            self._resolve_column(table, key)
            normalized_data[key] = self._normalize_value_for_column(entity, key, value)
        params = dict(where_params)
        stmt = sa_update(table).where(where_expr).values(**normalized_data)
        return self.run_native(stmt, params=params)

    def count(self, entity: str, where: Mapping[str, Any] | str | None = None) -> int:
        entity = self._validate_identifier(entity)
        table = self._get_table(entity)
        where_expr, params = self._build_where_expression(entity, table, where)
        stmt = select(func.count().label("total")).select_from(table)
        if where_expr is not None:
            stmt = stmt.where(where_expr)
        rows = self.run_native(stmt, params=params)
        if not rows:
            return 0
        value = rows[0].get("total", 0)
        return int(value)

    def _normalize_group_by(self, group_by: str | list[str] | tuple[str, ...] | None) -> list[str]:
        if group_by is None:
            return []
        if isinstance(group_by, str):
            fields = [group_by.strip()]
        elif isinstance(group_by, (list, tuple)):
            fields = [str(item).strip() for item in group_by]
        else:
            raise QueryError("group_by must be a string, list, tuple, or None")
        normalized: list[str] = []
        for field in fields:
            if not field:
                raise QueryError("group_by contains an empty field")
            normalized.append(self._validate_identifier(field))
        return normalized

    def _validate_aggregate_metric_format(self, alias: str, metric: Any) -> None:
        alias_name = self._validate_identifier(alias)
        if isinstance(metric, str):
            match = self._AGG_FUNC_RE.fullmatch(metric)
            if not match:
                raise QueryError("Invalid aggregate metric format; expected e.g. count(*), sum(field)")
            return
        if isinstance(metric, Mapping):
            op = str(metric.get("op", "")).strip().upper()
            field = str(metric.get("field", "*")).strip()
            if not op:
                raise QueryError(f"Aggregate metric '{alias_name}' requires op")
            if op not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
                raise QueryError(f"Unsupported aggregate op: {op}")
            if field != "*":
                self._validate_identifier(field)
            return
        raise QueryError("metrics values must be strings like 'count(*)' or mappings")

    def _normalize_aggregate_metric(self, alias: str, metric: Any, table: Table | None = None):
        alias_name = self._validate_identifier(alias)
        if isinstance(metric, str):
            match = self._AGG_FUNC_RE.fullmatch(metric)
            if not match:
                raise QueryError("Invalid aggregate metric format; expected e.g. count(*), sum(field)")
            op = match.group(1).upper()
            field = match.group(2)
        elif isinstance(metric, Mapping):
            op = str(metric.get("op", "")).strip().upper()
            field = str(metric.get("field", "*")).strip()
            if not op:
                raise QueryError(f"Aggregate metric '{alias_name}' requires op")
            if op not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
                raise QueryError(f"Unsupported aggregate op: {op}")
            if field != "*":
                self._validate_identifier(field)
        else:
            raise QueryError("metrics values must be strings like 'count(*)' or mappings")
        if field == "*" and op != "COUNT":
            raise QueryError(f"{op}(*) is not supported; use COUNT(*) or specify a field")
        if field == "*":
            metric_column = None
        else:
            if table is None:
                raise QueryError(f"Aggregate metric '{alias_name}' requires a resolved table")
            metric_column = self._resolve_column(table, field)
        aggregate_map = {
            "COUNT": func.count,
            "SUM": func.sum,
            "AVG": func.avg,
            "MIN": func.min,
            "MAX": func.max,
        }
        builder = aggregate_map[op]
        expression = builder() if metric_column is None else builder(metric_column)
        return expression.label(alias_name)

    def aggregate(
        self,
        entity: str,
        *,
        group_by: str | list[str] | tuple[str, ...] | None = None,
        metrics: Mapping[str, Any] | None = None,
        where: Mapping[str, Any] | str | None = None,
        having: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        pipeline: list[Mapping[str, Any]] | None = None,
    ) -> Any:
        if pipeline is not None:
            raise QueryError("pipeline is only supported for NoSQL aggregate")
        entity = self._validate_identifier(entity)
        group_fields = self._normalize_group_by(group_by)
        metrics = metrics or {}
        metric_aliases: dict[str, Any] = {}
        for alias, metric in metrics.items():
            self._validate_aggregate_metric_format(alias, metric)
        table = self._get_table(entity)
        group_columns = [self._resolve_column(table, field) for field in group_fields]
        select_parts = list(group_columns)
        if metrics:
            for alias, metric in metrics.items():
                metric_expr = self._normalize_aggregate_metric(alias, metric, table)
                metric_aliases[alias] = metric_expr
                select_parts.append(metric_expr)
        if not select_parts:
            raise QueryError("aggregate requires at least one group_by field or metric")
        where_expr, params = self._build_where_expression(entity, table, where)
        stmt = select(*select_parts).select_from(table)
        if where_expr is not None:
            stmt = stmt.where(where_expr)
        if group_fields:
            stmt = stmt.group_by(*group_columns)
        if isinstance(having, Mapping):
            having_conditions = []
            having_params: dict[str, Any] = {}
            for idx, (key, value) in enumerate(having.items()):
                self._validate_identifier(key)
                pname = f"h_{idx}"
                if key in metric_aliases:
                    having_conditions.append(metric_aliases[key] == bindparam(pname))
                    having_params[pname] = value
                else:
                    column = self._resolve_column(table, key)
                    having_conditions.append(column == bindparam(pname))
                    having_params[pname] = self._normalize_value_for_column(entity, key, value)
            having_expr = and_(*having_conditions) if having_conditions else None
        else:
            having_expr, having_params = self._build_having_expression(entity, table, having)
        if having_expr is not None:
            stmt = stmt.having(having_expr)
            params.update(having_params)
        if order_by:
            field, direction = self._parse_order_by_components(order_by)
            column = self._resolve_column(table, field)
            stmt = stmt.order_by(column.asc() if direction == "ASC" else column.desc())
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise QueryError("limit must be a positive integer")
            stmt = stmt.limit(limit)
        return self.run_native(stmt, params=params)

    def convert_uql(self, uql_query: str) -> Any:
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
            entity = self._validate_identifier(match.group(1))
            where = match.group(2)
            order_by = match.group(3)
            limit = int(match.group(4)) if match.group(4) else None
            sql = f"SELECT * FROM {self._quote(entity)}"  # nosec B608
            params: dict[str, Any] = {}
            if where:
                where_sql, params = self._build_parameterized_where_from_string(entity, where)
                sql += where_sql
            if order_by:
                sql += f" ORDER BY {self._validate_order_by_clause(order_by)}"
            if limit is not None:
                sql += " LIMIT :limit_value"
                params["limit_value"] = limit
            return sql, params
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
            where_sql, params = self._build_parameterized_where_from_string(entity, where)
            if not where_sql:
                raise QueryError("DELETE UQL requires WHERE")
            return f"DELETE FROM {self._quote(entity)}{where_sql}", params  # nosec B608
        if upper.startswith("CREATE "):
            match = re.match(r"CREATE\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{(.+)\}$", uql, flags=re.IGNORECASE)
            if not match:
                raise QueryError("Invalid CREATE UQL")
            entity = self._validate_identifier(match.group(1))
            parsed_data = self._parse_uql_create_body(match.group(2))
            self._ensure_table(entity, parsed_data)
            table = self._get_table(entity)
            normalized = {
                key: self._normalize_value_for_column(entity, key, value) for key, value in parsed_data.items()
            }
            return insert(table).values(**normalized), {}
        raise QueryError("Unsupported UQL command")

    def begin(self):
        return self._tx.begin()

    def commit(self) -> None:
        self._tx.commit()

    def rollback(self) -> None:
        self._tx.rollback()

    def transaction(self):
        return self._tx.transaction()

    def ping(self) -> Any:
        return self.run_native("SELECT 1")

    def close(self) -> None:
        self._conn_manager.dispose_engine(self.url)

    def create_view(self, name: str, select_query: str, *, replace: bool = False) -> Any:
        """Create a SQL view. Admin/internal use only."""
        view_name = self._validate_identifier(name)
        self._require_admin_mode("create_view")
        select_sql = self._validate_admin_sql_fragment(select_query, field_name="select_query")
        if not re.match(r"^(SELECT|WITH)\b", select_sql, flags=re.IGNORECASE):
            raise QueryError("view definition must start with SELECT or WITH")
        if self._DANGEROUS_SQL.search(select_sql):
            raise QueryError("select_query contains prohibited SQL content")
        if replace:
            self.drop_view(view_name, if_exists=True)
        sql = f"CREATE VIEW {self._quote(view_name)} AS {select_sql}"  # nosec B608
        return self.run_native(sql)

    def drop_view(self, name: str, *, if_exists: bool = True) -> Any:
        view_name = self._validate_identifier(name)
        sql = f"DROP VIEW {'IF EXISTS ' if if_exists else ''}{self._quote(view_name)}"  # nosec B608
        return self.run_native(sql)

    def create_procedure(self, name: str, definition: str, *, replace: bool = False) -> Any:
        """Create a stored procedure. Admin/internal use only."""
        proc_name = self._validate_identifier(name)
        if self.DIALECT == "sqlite":
            raise QueryError("stored procedures are not supported for sqlite")
        self._require_admin_mode("create_procedure")
        definition_sql = self._validate_admin_sql_fragment(definition, field_name="definition")
        if self._DANGEROUS_SQL.search(definition_sql):
            raise QueryError("definition contains prohibited SQL content")
        if replace:
            self.drop_procedure(proc_name, if_exists=True)
        keyword = "CREATE OR REPLACE PROCEDURE" if self.DIALECT == "postgres" else "CREATE PROCEDURE"
        sql = f"{keyword} {self._quote(proc_name)} {definition_sql}"  # nosec B608
        return self.run_native(sql)

    def drop_procedure(self, name: str, *, if_exists: bool = True) -> Any:
        proc_name = self._validate_identifier(name)
        if self.DIALECT == "sqlite":
            raise QueryError("stored procedures are not supported for sqlite")
        sql = f"DROP PROCEDURE {'IF EXISTS ' if if_exists else ''}{self._quote(proc_name)}"  # nosec B608
        return self.run_native(sql)

    def call_procedure(self, name: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        proc_name = self._validate_identifier(name)
        if self.DIALECT == "sqlite":
            raise QueryError("stored procedures are not supported for sqlite")
        values = list(params or [])
        placeholder_sql = ", ".join(f":p_{idx}" for idx in range(len(values)))
        bound = {f"p_{idx}": value for idx, value in enumerate(values)}
        keyword = "EXEC" if self.DIALECT == "mssql" else "CALL"
        sql = f"{keyword} {self._quote(proc_name)}"
        if placeholder_sql:
            sql += f" {placeholder_sql}" if self.DIALECT == "mssql" else f"({placeholder_sql})"
        return self.run_native(sql, params=bound)

    def create_function(self, name: str, definition: str, *, replace: bool = False) -> Any:
        """Create a SQL function. Admin/internal use only."""
        func_name = self._validate_identifier(name)
        if self.DIALECT == "sqlite":
            raise QueryError("function creation is not supported for sqlite")
        self._require_admin_mode("create_function")
        definition_sql = self._validate_admin_sql_fragment(definition, field_name="definition")
        if self._DANGEROUS_SQL.search(definition_sql):
            raise QueryError("definition contains prohibited SQL content")
        if replace:
            self.drop_function(func_name, if_exists=True)
        keyword = "CREATE OR REPLACE FUNCTION" if self.DIALECT == "postgres" else "CREATE FUNCTION"
        sql = f"{keyword} {self._quote(func_name)} {definition_sql}"  # nosec B608
        return self.run_native(sql)

    def drop_function(self, name: str, *, if_exists: bool = True) -> Any:
        func_name = self._validate_identifier(name)
        if self.DIALECT == "sqlite":
            raise QueryError("function dropping is not supported for sqlite")
        sql = f"DROP FUNCTION {'IF EXISTS ' if if_exists else ''}{self._quote(func_name)}"  # nosec B608
        return self.run_native(sql)

    def call_function(self, name: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        func_name = self._validate_identifier(name)
        values = list(params or [])
        bindings = [bindparam(f"p_{idx}", value=value) for idx, value in enumerate(values)]
        stmt = select(getattr(func, func_name)(*bindings).label("result"))
        rows = self.run_native(stmt)
        if not rows:
            return None
        return rows[0].get("result")

    def create_event(
        self,
        name: str,
        schedule: str,
        body: str,
        *,
        replace: bool = False,
        preserve: bool = True,
        enable: bool = True,
    ) -> Any:
        """Create a scheduled SQL event. Admin/internal use only."""
        event_name = self._validate_identifier(name)
        if self.DIALECT != "mysql":
            raise QueryError("database events are currently supported only for mysql")
        if not isinstance(schedule, str) or not schedule.strip():
            raise QueryError("schedule must be a non-empty SQL fragment")
        schedule_sql = schedule.strip()
        if not re.fullmatch(r"EVERY\s+\d+\s+(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH)", schedule_sql, flags=re.IGNORECASE):
            raise QueryError("schedule must match EVERY <n> <SECOND|MINUTE|HOUR|DAY|WEEK|MONTH>")
        self._require_admin_mode("create_event")
        body_sql = self._validate_admin_sql_fragment(body, field_name="body")
        if self._DANGEROUS_SQL.search(body_sql):
            raise QueryError("body contains prohibited SQL content")
        if replace:
            self.drop_event(event_name, if_exists=True)
        preserve_sql = "ON COMPLETION PRESERVE" if preserve else "ON COMPLETION NOT PRESERVE"
        status_sql = "ENABLE" if enable else "DISABLE"
        sql = (
            f"CREATE EVENT {self._quote(event_name)} "
            f"ON SCHEDULE {schedule_sql} "
            f"{preserve_sql} "
            f"{status_sql} "
            f"DO {body_sql}"
        )  # nosec B608
        return self.run_native(sql)

    def drop_event(self, name: str, *, if_exists: bool = True) -> Any:
        event_name = self._validate_identifier(name)
        if self.DIALECT != "mysql":
            raise QueryError("database events are currently supported only for mysql")
        sql = f"DROP EVENT {'IF EXISTS ' if if_exists else ''}{self._quote(event_name)}"  # nosec B608
        return self.run_native(sql)
