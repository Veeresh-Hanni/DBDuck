"""Microsoft SQL Server adapter."""

from __future__ import annotations

import re
from typing import Any, Mapping

from ..core.exceptions import QueryError
from ._sqlalchemy_adapter import SQLAlchemyAdapter


class MSSQLAdapter(SQLAlchemyAdapter):
    DIALECT = "mssql"

    def _quote(self, name: str) -> str:
        return f"[{name}]"

    def _pk_column_sql(self) -> str:
        return "[id] INT IDENTITY(1,1) PRIMARY KEY"

    def _type_for_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return "BIT"
        if isinstance(value, int):
            return "INT"
        if isinstance(value, float):
            return "FLOAT"
        return "NVARCHAR(255)"

    def _ensure_table(self, entity: str, data: Mapping[str, Any]) -> None:
        quoted_table = self._quote(entity)
        has_explicit_id = any(k.lower() == "id" for k in data)
        cols = [] if has_explicit_id else [self._pk_column_sql()]
        for key, value in data.items():
            cols.append(f"{self._quote(key)} {self._type_for_value(value)}")
        safe_entity = entity.replace("'", "''")
        sql = (
            f"IF OBJECT_ID(N'{safe_entity}', N'U') IS NULL "
            f"BEGIN CREATE TABLE {quoted_table} ({', '.join(cols)}) END"
        )
        self.run_native(sql)

    def _render_metric_sql(self, alias: str, metric: Any) -> str:
        self._validate_aggregate_metric_format(alias, metric)
        alias_name = self._validate_identifier(alias)
        if isinstance(metric, str):
            match = self._AGG_FUNC_RE.fullmatch(metric)
            if match is None:
                raise QueryError("Invalid aggregate metric format; expected e.g. count(*), sum(field)")
            op = match.group(1).upper()
            field = match.group(2)
        else:
            op = str(metric.get("op", "")).strip().upper()
            field = str(metric.get("field", "*")).strip()
        field_sql = "*" if field == "*" else self._quote(self._validate_identifier(field))
        return f"{op}({field_sql}) AS {self._quote(alias_name)}"

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self._find_with_offset(entity, where=where, order_by=order_by, limit=limit, offset=None)

    def _find_with_offset(
        self,
        entity: str,
        *,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Any:
        entity = self._validate_identifier(entity)
        where_sql, params = self._build_where_clause(entity, where)
        order_clause = self._validate_order_by_clause(order_by) if order_by else None
        if offset is None:
            top_sql = ""
            if limit is not None:
                if not isinstance(limit, int) or limit <= 0:
                    raise QueryError("limit must be a positive integer")
                top_sql = f" TOP {limit}"
            sql = f"SELECT{top_sql} * FROM {self._quote(entity)}{where_sql}"  # nosec B608
            if order_clause:
                sql += f" ORDER BY {order_clause}"
            return self.run_native(sql, params=params)
        if not isinstance(offset, int) or offset < 0:
            raise QueryError("offset must be a non-negative integer")
        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise QueryError("limit must be a positive integer")
        sql = f"SELECT * FROM {self._quote(entity)}{where_sql}"  # nosec B608
        sql += f" ORDER BY {order_clause or '(SELECT NULL)'}"
        sql += " OFFSET :offset_value ROWS"
        params["offset_value"] = offset
        if limit is not None:
            sql += " FETCH NEXT :limit_value ROWS ONLY"
            params["limit_value"] = limit
        return self.run_native(sql, params=params)

    def paginate(
        self,
        entity: str,
        *,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int,
        offset: int,
    ) -> Any:
        return self._find_with_offset(entity, where=where, order_by=order_by, limit=limit, offset=offset)

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
            top_sql = f" TOP {limit}" if limit is not None else ""
            sql = f"SELECT{top_sql} * FROM {self._quote(self._validate_identifier(entity))}"  # nosec B608
            if where:
                safe_where = self._validate_uql_where_clause(where)
                sql += f" WHERE {safe_where}"
            if order_by:
                sql += f" ORDER BY {self._validate_order_by_clause(order_by)}"
            return sql
        return super().convert_uql(uql_query)

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
        select_parts = [self._quote(field) for field in group_fields]
        if metrics:
            for alias, metric in metrics.items():
                select_parts.append(self._render_metric_sql(alias, metric))
        if not select_parts:
            raise QueryError("aggregate requires at least one group_by field or metric")
        top_sql = ""
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise QueryError("limit must be a positive integer")
            top_sql = f" TOP {limit}"
        where_sql, params = self._build_where_clause(entity, where)
        sql = f"SELECT{top_sql} {', '.join(str(part) for part in select_parts)} FROM {self._quote(entity)}{where_sql}"  # nosec B608
        if group_fields:
            sql += " GROUP BY " + ", ".join(self._quote(field) for field in group_fields)
        having_sql, having_params = self._build_having_clause(entity, having)
        if having_sql:
            sql += having_sql
            params.update(having_params)
        if order_by:
            sql += f" ORDER BY {self._validate_order_by_clause(order_by)}"
        return self.run_native(sql, params=params)
