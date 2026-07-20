import re

from sqlalchemy import create_engine, text

from .base_adapter import BaseAdapter
from ...core.exceptions import QueryError
from ...utils.logger import get_logger, log_internal_debug
from .sql._legacy_sql_common import ParameterizedSQL, literal_to_uql, parameterize_condition, parse_literal_value


class SQLAdapter(BaseAdapter):
    def __init__(self, url="sqlite:///test.db"):
        self.url = url
        self.engine = create_engine(url)
        self.dialect = self.engine.url.get_backend_name().lower()
        self._logger = get_logger()

    @staticmethod
    def _validate_identifier(name):
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Invalid SQL identifier: {name!r}")
        return name

    def _quote(self, name):
        if self.dialect in {"mysql", "mariadb"}:
            return f"`{name}`"
        if self.dialect in {"mssql"}:
            return f"[{name}]"
        return f'"{name}"'

    def _ensure_table(self, table_name, fields):
        safe_table_name = self._validate_identifier(str(table_name))
        has_explicit_id = any(str(name).lower() == "id" for name in fields)
        if self.dialect == "sqlite":
            pk = '"id" INTEGER PRIMARY KEY AUTOINCREMENT'
            text_type = "TEXT"
            bool_type = "INTEGER"
        elif self.dialect in {"mysql", "mariadb"}:
            pk = "`id` INT PRIMARY KEY AUTO_INCREMENT"
            text_type = "VARCHAR(255)"
            bool_type = "BOOLEAN"
        elif self.dialect in {"postgresql", "postgres"}:
            pk = '"id" SERIAL PRIMARY KEY'
            text_type = "TEXT"
            bool_type = "BOOLEAN"
        elif self.dialect == "mssql":
            pk = "[id] INT IDENTITY(1,1) PRIMARY KEY"
            text_type = "NVARCHAR(255)"
            bool_type = "BIT"
        else:
            pk = '"id" INT PRIMARY KEY'
            text_type = "TEXT"
            bool_type = "TEXT"

        cols = [] if has_explicit_id else [pk]
        for name, value in fields.items():
            self._validate_identifier(str(name))
            value = value.strip().strip('"').strip("'")
            qname = self._quote(name)
            if value.lower() in {"true", "false"}:
                cols.append(f"{qname} {bool_type}")
            elif value.isdigit():
                cols.append(f"{qname} INT")
            elif value.replace(".", "", 1).isdigit():
                cols.append(f"{qname} FLOAT")
            else:
                cols.append(f"{qname} {text_type}")

        if self.dialect == "mssql":
            check_stmt = text("SELECT OBJECT_ID(:tname, N'U') AS oid")
            with self.engine.begin() as conn:
                existing = conn.execute(check_stmt, {"tname": safe_table_name}).mappings().first()
                if existing and existing.get("oid") is not None:
                    return
            create_stmt = (
                f"CREATE TABLE {self._quote(safe_table_name)} ({', '.join(cols)});"
            )
        else:
            create_stmt = f"CREATE TABLE IF NOT EXISTS {self._quote(safe_table_name)} ({', '.join(cols)});"

        with self.engine.begin() as conn:
            conn.execute(text(create_stmt))

    def run_native(self, query, params=None):
        if isinstance(query, ParameterizedSQL) and params is None:
            params = query.params
        with self.engine.begin() as conn:
            try:
                result = conn.execute(text(str(query)), params or {})
                try:
                    return result.fetchall()
                except Exception:
                    return "Query executed successfully."
            except Exception as exc:
                log_internal_debug(
                    self._logger,
                    "Legacy SQLAdapter execution failed",
                    event="query.error.internal",
                    db=self.dialect,
                    exc=exc,
                )
                raise QueryError(str(exc) or exc.__class__.__name__) from exc

    def convert_uql(self, uql):
        uql = uql.strip()
        cmd = uql.upper()

        if cmd.startswith("FIND"):
            table, condition = self._extract_table_and_condition(uql)
            order_by = self._extract_order_by(uql)
            limit = self._extract_limit(uql)

            params = {}
            if self.dialect == "mssql" and limit:
                query = f"SELECT TOP {limit} * FROM {self._quote(table)}"  # nosec B608
            else:
                query = f"SELECT * FROM {self._quote(table)}"  # nosec B608

            if condition:
                where_sql, params = self._parameterize_condition(condition)
                query += f" WHERE {where_sql}"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit and self.dialect != "mssql":
                query += f" LIMIT {limit}"
            return ParameterizedSQL(query + ";", params)

        if cmd.startswith("CREATE"):
            table, fields = self._extract_table_and_body(uql)
            self._ensure_table(table, fields)

            columns = ", ".join([self._quote(c) for c in fields.keys()])
            placeholders = []
            params = {}
            for idx, value in enumerate(fields.values()):
                pname = f"v_{idx}"
                placeholders.append(f":{pname}")
                params[pname] = self._parse_literal_value(value)
            sql = f"INSERT INTO {self._quote(table)} ({columns}) VALUES ({', '.join(placeholders)});"  # nosec B608
            return ParameterizedSQL(sql, params)

        if cmd.startswith("DELETE"):
            table, condition = self._extract_table_and_condition(uql)
            normalized, params = self._parameterize_condition(condition or "1=1")
            sql = f"DELETE FROM {self._quote(table)} WHERE {normalized};"  # nosec B608
            return ParameterizedSQL(sql, params)

        return "/* Unsupported UQL syntax */"

    def _extract_table_and_condition(self, uql):
        match = re.match(
            r"(FIND|DELETE)\s+(\w+)"
            r"(?:\s+WHERE\s+(.+?))?"
            r"(?:\s+ORDER BY|\s+LIMIT|$)",
            uql,
            re.IGNORECASE,
        )
        if not match:
            return None, None
        return match.group(2), match.group(3)

    def _extract_table_and_body(self, uql):
        match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", uql, re.IGNORECASE)
        return match.group(1), self._parse_key_value_pairs(match.group(2))

    def _extract_order_by(self, uql):
        match = re.search(r"ORDER BY\s+(\w+)\s*(ASC|DESC)?", uql, re.IGNORECASE)
        if not match:
            return None
        field, direction = match.group(1), match.group(2) or "ASC"
        return self._validate_order_by_clause(f"{field} {direction}")

    def _extract_limit(self, uql):
        match = re.search(r"LIMIT\s+(\d+)", uql, re.IGNORECASE)
        return match.group(1) if match else None

    def _validate_order_by_clause(self, order_by):
        match = re.fullmatch(r"(-?)([A-Za-z_][A-Za-z0-9_]*)(?:\s+(ASC|DESC))?", order_by.strip(), re.IGNORECASE)
        if not match:
            raise QueryError("Invalid order_by clause")
        descending_prefix, field = match.group(1), match.group(2)
        direction = (match.group(3) or ("DESC" if descending_prefix else "ASC")).upper()
        return f"{self._quote(field)} {direction}"

    @staticmethod
    def _validate_limit(limit):
        if isinstance(limit, bool):
            raise QueryError("limit must be a positive integer")
        if isinstance(limit, int):
            value = limit
        elif isinstance(limit, str) and re.fullmatch(r"\d+", limit.strip()):
            value = int(limit.strip())
        else:
            raise QueryError("limit must be a positive integer")
        if value <= 0:
            raise QueryError("limit must be a positive integer")
        return str(value)

    def create(self, entity, data):
        entity_name = self._validate_identifier(str(entity))
        fields = {self._validate_identifier(str(k)): self._literal_to_uql(v) for k, v in dict(data).items()}
        query = self.convert_uql(
            "CREATE " + entity_name + " {" + ", ".join(f"{key}: {value}" for key, value in fields.items()) + "}"
        )
        return self.run_native(query)

    def create_many(self, entity, rows):
        total = 0
        for row in rows:
            result = self.create(entity, row)
            if isinstance(result, str):
                continue
            total += 1
        return {"rows_affected": total}

    def find(self, entity, where=None, order_by=None, limit=None):
        entity_name = self._validate_identifier(str(entity))
        query = f"SELECT * FROM {self._quote(entity_name)}"  # nosec B608
        params = {}
        if isinstance(where, dict) and where:
            where_sql, params = self._parameterize_condition(
                " AND ".join(f"{self._validate_identifier(str(key))} = {self._literal_to_uql(value)}" for key, value in where.items())
            )
            query += f" WHERE {where_sql}"
        elif isinstance(where, str) and where.strip():
            where_sql, params = self._parameterize_condition(where.strip())
            query += f" WHERE {where_sql}"
        if order_by:
            query += " ORDER BY " + self._validate_order_by_clause(str(order_by))
        if limit is not None:
            query += " LIMIT " + self._validate_limit(limit)
        return self.run_native(ParameterizedSQL(query + ";", params))

    def delete(self, entity, where):
        entity_name = self._validate_identifier(str(entity))
        query = f"DELETE FROM {self._quote(entity_name)}"  # nosec B608
        params = {}
        if isinstance(where, dict) and where:
            where_sql, params = self._parameterize_condition(
                " AND ".join(f"{self._validate_identifier(str(key))} = {self._literal_to_uql(value)}" for key, value in where.items())
            )
            query += f" WHERE {where_sql}"
        elif isinstance(where, str) and where.strip():
            where_sql, params = self._parameterize_condition(where.strip())
            query += f" WHERE {where_sql}"
        else:
            raise QueryError("delete requires a non-empty where condition")
        return self.run_native(ParameterizedSQL(query + ";", params))

    def update(self, entity, data, where):
        entity_name = self._validate_identifier(str(entity))
        if not data:
            raise QueryError("update data must be a non-empty mapping")
        assignments = []
        params = {}
        for idx, (key, value) in enumerate(dict(data).items()):
            field_name = self._validate_identifier(str(key))
            pname = f"u_{idx}"
            assignments.append(f"{self._quote(field_name)} = :{pname}")
            params[pname] = value
        if isinstance(where, dict) and where:
            where_sql, where_params = self._parameterize_condition(
                " AND ".join(
                    f"{self._validate_identifier(str(key))} = {self._literal_to_uql(value)}"
                    for key, value in where.items()
                )
            )
        elif isinstance(where, str) and where.strip():
            where_sql, where_params = self._parameterize_condition(where.strip())
        else:
            raise QueryError("update requires a non-empty where condition")
        sql = f"UPDATE {self._quote(entity_name)} SET {', '.join(assignments)} WHERE {where_sql}"  # nosec B608
        params.update(where_params)
        return self.run_native(ParameterizedSQL(sql, params))

    def count(self, entity, where=None):
        entity_name = self._validate_identifier(str(entity))
        params = {}
        where_sql = ""
        if isinstance(where, dict) and where:
            where_sql, params = self._parameterize_condition(
                " AND ".join(
                    f"{self._validate_identifier(str(key))} = {self._literal_to_uql(value)}"
                    for key, value in where.items()
                )
            )
        elif isinstance(where, str) and where.strip():
            where_sql, params = self._parameterize_condition(where.strip())
        sql = f"SELECT COUNT(*) AS total FROM {self._quote(entity_name)}"  # nosec B608
        if where_sql:
            sql += f" WHERE {where_sql}"
        return self.run_native(ParameterizedSQL(sql, params))

    def _parse_key_value_pairs(self, fields):
        result = {}
        for pair in fields.split(","):
            key, val = pair.split(":", 1)
            result[key.strip()] = val.strip()
        return result

    @staticmethod
    def _literal_to_uql(value):
        return literal_to_uql(value)

    def _parameterize_condition(self, condition):
        return parameterize_condition(condition, quote_identifier=self._quote, normalize_condition=self._normalize_condition)

    def _normalize_condition(self, condition):
        if not condition:
            return condition

        if self.dialect in {"mssql", "sqlite", "mysql", "mariadb"}:
            condition = re.sub(r"\btrue\b", "1", condition, flags=re.IGNORECASE)
            condition = re.sub(r"\bfalse\b", "0", condition, flags=re.IGNORECASE)
        return condition

    def _format_value(self, val):
        val = val.strip('"').strip("'")
        lower_val = val.lower()

        if lower_val in {"true", "false"}:
            if self.dialect in {"mssql", "sqlite", "mysql", "mariadb"}:
                return "1" if lower_val == "true" else "0"
            return lower_val

        if val.isdigit() or val.replace(".", "", 1).isdigit():
            return val

        safe = val.replace("'", "''")
        return f"'{safe}'"

    @staticmethod
    def _parse_literal_value(raw):
        return parse_literal_value(raw)
