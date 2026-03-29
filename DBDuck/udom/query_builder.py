"""Fluent Query Builder for UDOM.

Provides a chainable DSL for building queries across all database backends.
This is an additive API that works alongside the existing UDOM methods.

Example usage:
    db.table("users").where(active=True).order("name").limit(10).find()
    db.table("users").where(id=1).first()
    db.table("users").select("id", "name").where(active=True).find()
"""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any, Mapping

from ..core.exceptions import QueryError

if TYPE_CHECKING:
    from .udom import UDOM


class QueryBuilder:
    """Fluent query builder for constructing database queries.

    Supports method chaining for building complex queries in a readable way.
    All chainable methods return self for continued chaining.
    Terminal methods (find, first, count, update, delete) execute the query.
    """

    def __init__(self, udom: "UDOM", entity: str) -> None:
        """Initialize QueryBuilder with a UDOM instance and entity name.

        Args:
            udom: The UDOM instance to execute queries against.
            entity: The table/collection/label name to query.
        """
        self._udom = udom
        self._entity = entity
        self._where_conditions: dict[str, Any] = {}
        self._or_conditions: list[dict[str, Any]] = []
        self._order_by: str | None = None
        self._limit_value: int | None = None
        self._offset_value: int | None = None
        self._select_fields: list[str] | None = None
        self._group_by_fields: str | list[str] | None = None
        self._having_conditions: dict[str, Any] | str | None = None
        self._metrics: dict[str, Any] | None = None
        self._joins: list[dict[str, Any]] = []

    def where(self, conditions: Mapping[str, Any] | str | None = None, **kwargs: Any) -> "QueryBuilder":
        """Add WHERE conditions to the query (AND logic).

        Args:
            conditions: A mapping of field conditions or a string condition.
            **kwargs: Additional field=value conditions.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where(active=True, role="admin")
            db.table("users").where({"active": True})
        """
        if conditions is not None:
            if isinstance(conditions, str):
                self._where_conditions["__raw__"] = conditions
            elif isinstance(conditions, Mapping):
                self._where_conditions.update(conditions)
        if kwargs:
            self._where_conditions.update(kwargs)
        return self

    def where_or(self, *condition_groups: Mapping[str, Any]) -> "QueryBuilder":
        """Add OR conditions to the query.

        Each argument is a group of AND conditions, groups are OR'd together.

        Args:
            *condition_groups: Multiple condition mappings to OR together.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_or({"role": "admin"}, {"role": "superuser"})
        """
        for group in condition_groups:
            if isinstance(group, Mapping) and group:
                self._or_conditions.append(dict(group))
        return self

    def where_in(self, field: str, values: list[Any]) -> "QueryBuilder":
        """Add an IN condition for a field.

        Args:
            field: The field name.
            values: List of values to match.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_in("id", [1, 2, 3])
        """
        if not isinstance(values, (list, tuple)):
            raise ValueError("values must be a list or tuple")
        self._where_conditions[f"{field}__in"] = list(values)
        return self

    def where_not(self, **kwargs: Any) -> "QueryBuilder":
        """Add NOT conditions to the query.

        Args:
            **kwargs: Field=value conditions to negate.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_not(role="guest")
        """
        for key, value in kwargs.items():
            self._where_conditions[f"{key}__ne"] = value
        return self

    def where_gt(self, **kwargs: Any) -> "QueryBuilder":
        """Add greater-than conditions.

        Args:
            **kwargs: Field=value conditions for greater-than comparison.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_gt(age=18)
        """
        for key, value in kwargs.items():
            self._where_conditions[f"{key}__gt"] = value
        return self

    def where_gte(self, **kwargs: Any) -> "QueryBuilder":
        """Add greater-than-or-equal conditions.

        Args:
            **kwargs: Field=value conditions for >= comparison.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_gte(age=21)
        """
        for key, value in kwargs.items():
            self._where_conditions[f"{key}__gte"] = value
        return self

    def where_lt(self, **kwargs: Any) -> "QueryBuilder":
        """Add less-than conditions.

        Args:
            **kwargs: Field=value conditions for less-than comparison.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_lt(age=65)
        """
        for key, value in kwargs.items():
            self._where_conditions[f"{key}__lt"] = value
        return self

    def where_lte(self, **kwargs: Any) -> "QueryBuilder":
        """Add less-than-or-equal conditions.

        Args:
            **kwargs: Field=value conditions for <= comparison.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_lte(age=30)
        """
        for key, value in kwargs.items():
            self._where_conditions[f"{key}__lte"] = value
        return self

    def where_like(self, **kwargs: Any) -> "QueryBuilder":
        """Add LIKE pattern conditions (SQL) or regex (NoSQL).

        Args:
            **kwargs: Field=pattern conditions.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_like(name="%john%")
        """
        for key, value in kwargs.items():
            self._where_conditions[f"{key}__like"] = value
        return self

    def where_null(self, *fields: str) -> "QueryBuilder":
        """Add IS NULL conditions for fields.

        Args:
            *fields: Field names to check for NULL.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_null("deleted_at")
        """
        for field in fields:
            self._where_conditions[f"{field}__null"] = True
        return self

    def where_not_null(self, *fields: str) -> "QueryBuilder":
        """Add IS NOT NULL conditions for fields.

        Args:
            *fields: Field names to check for NOT NULL.

        Returns:
            self for method chaining.

        Example:
            db.table("users").where_not_null("email")
        """
        for field in fields:
            self._where_conditions[f"{field}__notnull"] = True
        return self

    def select(self, *fields: str) -> "QueryBuilder":
        """Specify which fields to return (projection).

        Args:
            *fields: Field names to include in results.

        Returns:
            self for method chaining.

        Example:
            db.table("users").select("id", "name", "email").find()
        """
        if fields:
            self._select_fields = list(fields)
        return self

    def order(self, field: str, direction: str = "ASC") -> "QueryBuilder":
        """Set the ORDER BY clause.

        Args:
            field: The field name to order by.
            direction: Sort direction ("ASC" or "DESC").

        Returns:
            self for method chaining.

        Example:
            db.table("users").order("created_at", "DESC")
        """
        direction = direction.upper().strip()
        if direction not in ("ASC", "DESC"):
            raise ValueError("direction must be 'ASC' or 'DESC'")
        self._order_by = f"{field} {direction}" if direction == "DESC" else field
        return self

    def order_by(self, order_expr: str) -> "QueryBuilder":
        """Set the ORDER BY clause with a raw expression.

        Args:
            order_expr: The order expression (e.g., "name DESC", "created_at").

        Returns:
            self for method chaining.

        Example:
            db.table("users").order_by("created_at DESC")
        """
        self._order_by = order_expr
        return self

    def limit(self, count: int) -> "QueryBuilder":
        """Set the maximum number of results to return.

        Args:
            count: Maximum number of records.

        Returns:
            self for method chaining.

        Example:
            db.table("users").limit(10).find()
        """
        if not isinstance(count, int) or count <= 0:
            raise ValueError("limit must be a positive integer")
        self._limit_value = count
        return self

    def offset(self, count: int) -> "QueryBuilder":
        """Set the number of records to skip.

        Args:
            count: Number of records to skip.

        Returns:
            self for method chaining.

        Example:
            db.table("users").offset(20).limit(10).find()
        """
        if not isinstance(count, int) or count < 0:
            raise ValueError("offset must be a non-negative integer")
        self._offset_value = count
        return self

    def page(self, page_num: int, page_size: int = 20) -> "QueryBuilder":
        """Set pagination using page number and size.

        Args:
            page_num: The page number (1-indexed).
            page_size: Number of records per page.

        Returns:
            self for method chaining.

        Example:
            db.table("users").page(2, 25).find()
        """
        if not isinstance(page_num, int) or page_num < 1:
            raise ValueError("page_num must be a positive integer")
        if not isinstance(page_size, int) or page_size < 1:
            raise ValueError("page_size must be a positive integer")
        self._offset_value = (page_num - 1) * page_size
        self._limit_value = page_size
        return self

    def group_by(self, *fields: str) -> "QueryBuilder":
        """Set GROUP BY fields for aggregation.

        Args:
            *fields: Field names to group by.

        Returns:
            self for method chaining.

        Example:
            db.table("orders").group_by("status").aggregate(metrics={"total": "count"})
        """
        if len(fields) == 1:
            self._group_by_fields = fields[0]
        else:
            self._group_by_fields = list(fields)
        return self

    def join(
        self,
        entity: str,
        *,
        on: Mapping[str, str] | tuple[str, str] | list[str],
        join_type: str = "inner",
    ) -> "QueryBuilder":
        """Add a SQL join clause.

        Args:
            entity: The table name to join.
            on: Join mapping from base/current field to target field.
                Examples: ``{"id": "user_id"}`` or ``("id", "user_id")``.
            join_type: ``"inner"`` or ``"left"``.

        Returns:
            self for method chaining.
        """
        normalized_type = join_type.lower().strip()
        if normalized_type not in {"inner", "left"}:
            raise ValueError("join_type must be 'inner' or 'left'")
        if isinstance(on, Mapping):
            pairs = [(str(left), str(right)) for left, right in on.items()]
        elif isinstance(on, (tuple, list)) and len(on) == 2:
            pairs = [(str(on[0]), str(on[1]))]
        else:
            raise ValueError("on must be a mapping or a 2-item tuple/list")
        if not pairs:
            raise ValueError("join requires at least one join condition")
        self._joins.append(
            {
                "entity": str(entity).strip(),
                "on": pairs,
                "type": normalized_type,
            }
        )
        return self

    def left_join(
        self,
        entity: str,
        *,
        on: Mapping[str, str] | tuple[str, str] | list[str],
    ) -> "QueryBuilder":
        """Add a LEFT JOIN clause."""
        return self.join(entity, on=on, join_type="left")

    def having(self, conditions: Mapping[str, Any] | str) -> "QueryBuilder":
        """Set HAVING conditions for aggregation.

        Args:
            conditions: HAVING clause conditions.

        Returns:
            self for method chaining.

        Example:
            db.table("orders").group_by("user_id").having({"count": {"$gt": 5}})
        """
        self._having_conditions = conditions
        return self

    def metrics(self, **kwargs: Any) -> "QueryBuilder":
        """Set aggregation metrics.

        Args:
            **kwargs: Metric definitions (e.g., total="count", avg_price="avg:price").

        Returns:
            self for method chaining.

        Example:
            db.table("orders").group_by("status").metrics(total="count", avg_amount="avg:amount")
        """
        self._metrics = kwargs
        return self

    def _build_where(self) -> Mapping[str, Any] | str | None:
        """Build the final where condition from all accumulated conditions."""
        if not self._where_conditions and not self._or_conditions:
            return None

        if "__raw__" in self._where_conditions:
            return str(self._where_conditions["__raw__"])

        if self._or_conditions and not self._where_conditions:
            if len(self._or_conditions) == 1:
                return self._or_conditions[0]
            return {"$or": self._or_conditions}

        base_conditions = {k: v for k, v in self._where_conditions.items() if k != "__raw__"}

        if self._or_conditions:
            return {"$and": [base_conditions, {"$or": self._or_conditions}]}

        return base_conditions if base_conditions else None

    def _apply_projection(self, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply field projection to results if select fields are specified."""
        if not self._select_fields or not results:
            return results
        projected = []
        for row in results:
            if isinstance(row, Mapping):
                projected.append({k: row.get(k) for k in self._select_fields if k in row})
            else:
                projected.append(row)
        return projected

    @staticmethod
    def _split_lookup(raw_key: str) -> tuple[str, str]:
        if "__" not in raw_key:
            return raw_key, "eq"
        field, lookup = raw_key.rsplit("__", 1)
        if not field:
            raise QueryError("Invalid where field lookup")
        return field, lookup.lower()

    def _require_sql_joins(self):
        if not self._joins:
            return None
        if self._udom.db_type != "sql":
            raise QueryError("join() is currently supported for SQL backends only")
        adapter = self._udom.adapter
        required = ("_get_table", "_resolve_column", "_normalize_value_for_column", "_validate_identifier")
        if not all(hasattr(adapter, attr) for attr in required):
            raise QueryError("Current SQL adapter does not support query builder joins")
        return adapter

    def _resolve_field_reference(self, table_map: Mapping[str, Any], raw_ref: str) -> tuple[str, str, Any]:
        if "." in raw_ref:
            table_name, field_name = raw_ref.split(".", 1)
            table_name = table_name.strip()
            field_name = field_name.strip()
        else:
            table_name = self._entity
            field_name = raw_ref.strip()
        if table_name not in table_map:
            raise QueryError(f"Unknown join entity '{table_name}'")
        table = table_map[table_name]
        column = self._udom.adapter._resolve_column(table, field_name)
        return table_name, field_name, column

    def _build_joined_from_clause(self):
        adapter = self._require_sql_joins()
        if adapter is None:
            return None, {}
        base_table = adapter._get_table(self._entity)
        table_map: dict[str, Any] = {self._entity: base_table}
        from_clause = base_table
        current_entity = self._entity
        current_table = base_table
        for spec in self._joins:
            join_entity = adapter._validate_identifier(spec["entity"])
            join_table = adapter._get_table(join_entity)
            table_map[join_entity] = join_table
            predicates = []
            for left_ref, right_ref in spec["on"]:
                left_expr = left_ref if "." in left_ref else f"{current_entity}.{left_ref}"
                right_expr = right_ref if "." in right_ref else f"{join_entity}.{right_ref}"
                _, _, left_column = self._resolve_field_reference(table_map, left_expr)
                _, _, right_column = self._resolve_field_reference(table_map, right_expr)
                predicates.append(left_column == right_column)
            from sqlalchemy import and_

            on_clause = and_(*predicates)
            if spec["type"] == "left":
                from_clause = from_clause.outerjoin(join_table, on_clause)
            else:
                from_clause = from_clause.join(join_table, on_clause)
            current_entity = join_entity
            current_table = join_table
        return from_clause, table_map

    def _build_join_where_expression(
        self,
        table_map: Mapping[str, Any],
        where: Mapping[str, Any] | str | None,
        *,
        param_index: int = 0,
    ):
        from sqlalchemy import and_, bindparam, or_, text

        adapter = self._require_sql_joins()
        if adapter is None:
            return None, {}
        if where is None:
            return None, {}
        if isinstance(where, str):
            if self._joins:
                raise QueryError("string where clauses are not supported for joined queries; use a mapping")
            where_sql, params = adapter._build_parameterized_where_from_string(self._entity, where)
            if not where_sql:
                return None, {}
            return text(where_sql.removeprefix(" WHERE ")), params
        if not isinstance(where, Mapping):
            raise QueryError("where must be a mapping, string, or None")

        conditions = []
        params: dict[str, Any] = {}
        next_param = param_index

        for key, value in where.items():
            key_text = str(key)
            if key_text == "$and":
                if not isinstance(value, (list, tuple)) or not value:
                    raise QueryError("$and requires a non-empty list of condition mappings")
                nested_parts = []
                for item in value:
                    if not isinstance(item, Mapping):
                        raise QueryError("$and entries must be mappings")
                    nested_expr, nested_params = self._build_join_where_expression(
                        table_map,
                        item,
                        param_index=next_param,
                    )
                    next_param += len(nested_params)
                    if nested_expr is not None:
                        nested_parts.append(nested_expr)
                        params.update(nested_params)
                if nested_parts:
                    conditions.append(and_(*nested_parts))
                continue
            if key_text == "$or":
                if not isinstance(value, (list, tuple)) or not value:
                    raise QueryError("$or requires a non-empty list of condition mappings")
                nested_parts = []
                for item in value:
                    if not isinstance(item, Mapping):
                        raise QueryError("$or entries must be mappings")
                    nested_expr, nested_params = self._build_join_where_expression(
                        table_map,
                        item,
                        param_index=next_param,
                    )
                    next_param += len(nested_params)
                    if nested_expr is not None:
                        nested_parts.append(nested_expr)
                        params.update(nested_params)
                if nested_parts:
                    conditions.append(or_(*nested_parts))
                continue

            field_ref, lookup = self._split_lookup(key_text)
            entity_name, field_name, column = self._resolve_field_reference(table_map, field_ref)

            if lookup == "null":
                conditions.append(column.is_(None) if value else column.is_not(None))
                continue
            if lookup == "notnull":
                conditions.append(column.is_not(None) if value else column.is_(None))
                continue
            if lookup == "in":
                if not isinstance(value, (list, tuple)):
                    raise QueryError(f"Field '{field_ref}' requires a list/tuple for __in lookup")
                normalized_values = [
                    adapter._normalize_value_for_column(entity_name, field_name, item) for item in value
                ]
                conditions.append(column.in_(normalized_values))
                continue

            operator_map = {
                "eq": lambda col, param: col == bindparam(param),
                "ne": lambda col, param: col != bindparam(param),
                "gt": lambda col, param: col > bindparam(param),
                "gte": lambda col, param: col >= bindparam(param),
                "lt": lambda col, param: col < bindparam(param),
                "lte": lambda col, param: col <= bindparam(param),
                "like": lambda col, param: col.like(bindparam(param)),
            }
            if lookup not in operator_map:
                raise QueryError(f"Unsupported lookup '__{lookup}' for field '{field_ref}'")

            pname = f"jw_{next_param}"
            next_param += 1
            params[pname] = adapter._normalize_value_for_column(entity_name, field_name, value)
            conditions.append(operator_map[lookup](column, pname))

        if not conditions:
            return None, {}
        return and_(*conditions), params

    def _column_output_key(self, entity_name: str, field_name: str, duplicate_counts: Mapping[str, int]) -> str:
        if entity_name == self._entity and duplicate_counts.get(field_name, 0) <= 1:
            return field_name
        return f"{entity_name}.{field_name}"

    def _build_join_select_parts(self, table_map: Mapping[str, Any]):
        select_parts = []
        if self._select_fields:
            for field in self._select_fields:
                entity_name, field_name, column = self._resolve_field_reference(table_map, field)
                label = field if "." in field else field_name
                select_parts.append(column.label(label))
            return select_parts

        duplicate_counts = Counter()
        for entity_name, table in table_map.items():
            for column in table.c:
                duplicate_counts[column.name] += 1
        for entity_name, table in table_map.items():
            for column in table.c:
                label = self._column_output_key(entity_name, column.name, duplicate_counts)
                select_parts.append(column.label(label))
        return select_parts

    def _parse_join_order(self, table_map: Mapping[str, Any]) -> Any | None:
        if not self._order_by:
            return None
        text = self._order_by.strip()
        import re

        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)(?:\s+(ASC|DESC))?", text, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid order_by clause")
        field_ref = match.group(1)
        direction = (match.group(2) or "ASC").upper()
        _, _, column = self._resolve_field_reference(table_map, field_ref)
        return column.asc() if direction == "ASC" else column.desc()

    def _find_with_joins(self) -> list[dict[str, Any]]:
        adapter = self._require_sql_joins()
        if adapter is None:
            return []
        from sqlalchemy import select

        from_clause, table_map = self._build_joined_from_clause()
        where_expr, params = self._build_join_where_expression(table_map, self._build_where())
        select_parts = self._build_join_select_parts(table_map)
        stmt = select(*select_parts).select_from(from_clause)
        if where_expr is not None:
            stmt = stmt.where(where_expr)
        order_clause = self._parse_join_order(table_map)
        if order_clause is not None:
            stmt = stmt.order_by(order_clause)
        if self._limit_value is not None:
            stmt = stmt.limit(self._limit_value)
        if self._offset_value is not None:
            stmt = stmt.offset(self._offset_value)
        results = adapter.run_native(stmt, params=params)
        return results if isinstance(results, list) else []

    def _count_with_joins(self) -> int:
        adapter = self._require_sql_joins()
        if adapter is None:
            return 0
        from sqlalchemy import func, select

        from_clause, table_map = self._build_joined_from_clause()
        where_expr, params = self._build_join_where_expression(table_map, self._build_where())
        stmt = select(func.count().label("total")).select_from(from_clause)
        if where_expr is not None:
            stmt = stmt.where(where_expr)
        rows = adapter.run_native(stmt, params=params)
        if not rows:
            return 0
        return int(rows[0].get("total", 0))

    # ─────────────────────────────────────────────────────────────────────────
    # Terminal Methods (execute the query)
    # ─────────────────────────────────────────────────────────────────────────

    def find(self) -> list[dict[str, Any]]:
        """Execute the query and return all matching records.

        Returns:
            List of matching records.

        Example:
            users = db.table("users").where(active=True).find()
        """
        if self._joins:
            results = self._find_with_joins()
        elif self._offset_value is not None:
            if self._limit_value is None:
                raise ValueError("offset/page requires limit or page_size before find()")
            if hasattr(self._udom.adapter, "paginate"):
                results = self._udom.adapter.paginate(
                    self._entity,
                    where=self._build_where(),
                    order_by=self._order_by,
                    limit=self._limit_value,
                    offset=self._offset_value,
                )
            else:
                expanded = self._udom.find(
                    self._entity,
                    where=self._build_where(),
                    order_by=self._order_by,
                    limit=self._offset_value + self._limit_value,
                )
                results = expanded[self._offset_value : self._offset_value + self._limit_value] if isinstance(expanded, list) else expanded
        else:
            results = self._udom.find(
                self._entity,
                where=self._build_where(),
                order_by=self._order_by,
                limit=self._limit_value,
            )
        if isinstance(results, list):
            return self._apply_projection(results)
        return results

    def first(self) -> dict[str, Any] | None:
        """Execute the query and return the first matching record.

        Returns:
            The first matching record or None.

        Example:
            user = db.table("users").where(id=1).first()
        """
        if self._joins:
            results = self.clone().limit(1).find()
        else:
            results = self._udom.find(
                self._entity,
                where=self._build_where(),
                order_by=self._order_by,
                limit=1,
            )
        if isinstance(results, list) and results:
            row = results[0]
            if self._select_fields and isinstance(row, Mapping):
                return {k: row.get(k) for k in self._select_fields if k in row}
            return row
        return None

    def count(self) -> int:
        """Execute the query and return the count of matching records.

        Returns:
            Number of matching records.

        Example:
            total = db.table("users").where(active=True).count()
        """
        if self._joins:
            return self._count_with_joins()
        return self._udom.count(self._entity, where=self._build_where())

    def exists(self) -> bool:
        """Check if any records match the query conditions.

        Returns:
            True if at least one record matches, False otherwise.

        Example:
            if db.table("users").where(email="test@example.com").exists():
                print("User already exists")
        """
        return self.count() > 0

    def update(self, data: Mapping[str, Any]) -> Any:
        """Execute an UPDATE with the built WHERE conditions.

        Args:
            data: The data to update.

        Returns:
            Update result from the adapter.

        Example:
            db.table("users").where(id=1).update({"name": "New Name"})
        """
        where = self._build_where()
        if where is None:
            raise ValueError("update requires at least one where condition")
        return self._udom.update(self._entity, data, where=where)

    def delete(self) -> Any:
        """Execute a DELETE with the built WHERE conditions.

        Returns:
            Delete result from the adapter.

        Example:
            db.table("users").where(id=1).delete()
        """
        where = self._build_where()
        if where is None:
            raise ValueError("delete requires at least one where condition")
        return self._udom.delete(self._entity, where=where)

    def create(self, data: Mapping[str, Any]) -> Any:
        """Insert a new record.

        Args:
            data: The data to insert.

        Returns:
            Create result from the adapter.

        Example:
            db.table("users").create({"name": "John", "email": "john@example.com"})
        """
        return self._udom.create(self._entity, data)

    def create_many(self, rows: list[Mapping[str, Any]]) -> Any:
        """Insert multiple records.

        Args:
            rows: List of data mappings to insert.

        Returns:
            Create result from the adapter.

        Example:
            db.table("users").create_many([{"name": "A"}, {"name": "B"}])
        """
        return self._udom.create_many(self._entity, rows)

    def aggregate(
        self,
        *,
        metrics: Mapping[str, Any] | None = None,
        pipeline: list[Mapping[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute an aggregation query.

        Args:
            metrics: Aggregation metrics (overrides builder metrics if provided).
            pipeline: Raw aggregation pipeline (for MongoDB).

        Returns:
            Aggregation results.

        Example:
            db.table("orders").group_by("status").metrics(total="count").aggregate()
        """
        final_metrics = metrics or self._metrics
        return self._udom.aggregate(
            self._entity,
            group_by=self._group_by_fields,
            metrics=final_metrics,
            where=self._build_where(),
            having=self._having_conditions,
            order_by=self._order_by,
            limit=self._limit_value,
            pipeline=pipeline,
        )

    def find_page(self, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        """Execute a paginated query.

        Args:
            page: Page number (1-indexed).
            page_size: Records per page.

        Returns:
            Pagination result with items, page, page_size, total, total_pages.

        Example:
            result = db.table("users").where(active=True).find_page(page=2, page_size=25)
        """
        return self._udom.find_page(
            self._entity,
            page=page,
            page_size=page_size,
            where=self._build_where(),
            order_by=self._order_by,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Vector-specific methods
    # ─────────────────────────────────────────────────────────────────────────

    def search_similar(self, vector: list[float], top_k: int = 10) -> list[dict[str, Any]]:
        """Search for similar vectors (vector databases only).

        Args:
            vector: The query vector.
            top_k: Number of results to return.

        Returns:
            List of similar items with scores.

        Example:
            db.table("embeddings").where(category="tech").search_similar([0.1, 0.2, 0.3], top_k=5)
        """
        return self._udom.search_similar(
            self._entity,
            vector=vector,
            top_k=top_k,
            filter=self._build_where(),
        )

    def upsert_vector(self, id: Any, vector: list[float], metadata: Mapping[str, Any] | None = None) -> Any:
        """Upsert a vector (vector databases only).

        Args:
            id: The vector ID.
            vector: The vector values.
            metadata: Optional metadata.

        Returns:
            Upsert result.

        Example:
            db.table("embeddings").upsert_vector("v1", [0.1, 0.2, 0.3], {"label": "test"})
        """
        return self._udom.upsert_vector(self._entity, id=id, vector=vector, metadata=metadata)

    # ─────────────────────────────────────────────────────────────────────────
    # Graph-specific methods
    # ─────────────────────────────────────────────────────────────────────────

    def find_related(
        self,
        id: Any,
        rel_type: str,
        direction: str = "out",
        target_label: str | None = None,
    ) -> list[dict[str, Any]]:
        """Find related nodes (graph databases only).

        Args:
            id: The source node ID.
            rel_type: The relationship type.
            direction: Relationship direction ("out", "in", "both").
            target_label: Optional target node label filter.

        Returns:
            List of related nodes.

        Example:
            db.table("User").where(id="u1").find_related(id="u1", rel_type="FOLLOWS")
        """
        return self._udom.find_related(
            self._entity,
            id=id,
            rel_type=rel_type,
            direction=direction,
            target_label=target_label,
        )

    def create_relationship(
        self,
        from_id: Any,
        rel_type: str,
        to_label: str,
        to_id: Any,
        props: Mapping[str, Any] | None = None,
    ) -> Any:
        """Create a relationship between nodes (graph databases only).

        Args:
            from_id: Source node ID.
            rel_type: Relationship type.
            to_label: Target node label.
            to_id: Target node ID.
            props: Optional relationship properties.

        Returns:
            Relationship creation result.

        Example:
            db.table("User").create_relationship("u1", "FOLLOWS", "User", "u2")
        """
        return self._udom.create_relationship(
            self._entity,
            from_id,
            rel_type,
            to_label,
            to_id,
            props,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Utility methods
    # ─────────────────────────────────────────────────────────────────────────

    def clone(self) -> "QueryBuilder":
        """Create a copy of this QueryBuilder with the same state.

        Returns:
            A new QueryBuilder instance with copied state.

        Example:
            base_query = db.table("users").where(active=True)
            admins = base_query.clone().where(role="admin").find()
            users = base_query.clone().where(role="user").find()
        """
        new_builder = QueryBuilder(self._udom, self._entity)
        new_builder._where_conditions = dict(self._where_conditions)
        new_builder._or_conditions = [dict(c) for c in self._or_conditions]
        new_builder._order_by = self._order_by
        new_builder._limit_value = self._limit_value
        new_builder._offset_value = self._offset_value
        new_builder._select_fields = list(self._select_fields) if self._select_fields else None
        new_builder._group_by_fields = self._group_by_fields
        new_builder._having_conditions = self._having_conditions
        new_builder._metrics = dict(self._metrics) if self._metrics else None
        new_builder._joins = [
            {"entity": item["entity"], "on": list(item["on"]), "type": item["type"]}
            for item in self._joins
        ]
        return new_builder

    def to_dict(self) -> dict[str, Any]:
        """Return the current query state as a dictionary.

        Returns:
            Dictionary representation of the query.

        Example:
            query_state = db.table("users").where(active=True).limit(10).to_dict()
        """
        return {
            "entity": self._entity,
            "where": self._build_where(),
            "order_by": self._order_by,
            "limit": self._limit_value,
            "offset": self._offset_value,
            "select": self._select_fields,
            "group_by": self._group_by_fields,
            "having": self._having_conditions,
            "metrics": self._metrics,
            "joins": [
                {"entity": item["entity"], "on": list(item["on"]), "type": item["type"]}
                for item in self._joins
            ],
        }

    def __repr__(self) -> str:
        """Return a string representation of the QueryBuilder."""
        parts = [f"QueryBuilder('{self._entity}')"]
        if self._where_conditions:
            parts.append(f".where({self._where_conditions})")
        if self._or_conditions:
            parts.append(f".where_or({self._or_conditions})")
        if self._order_by:
            parts.append(f".order_by('{self._order_by}')")
        if self._limit_value:
            parts.append(f".limit({self._limit_value})")
        if self._offset_value:
            parts.append(f".offset({self._offset_value})")
        if self._select_fields:
            parts.append(f".select({self._select_fields})")
        if self._joins:
            parts.append(f".joins({self._joins})")
        return "".join(parts)
