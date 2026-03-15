import re
from typing import Any

from sqlalchemy import create_engine, text

from .base_adapter import BaseAdapter


class ParameterizedSQL(str):
    def __new__(cls, sql: str, params: dict[str, Any] | None = None):
        obj = super().__new__(cls, sql)
        obj.params = params or {}
        return obj


class SQLAdapter(BaseAdapter):
    def __init__(self, url="sqlite:///test.db"):
        self.url = url
        self.engine = create_engine(url)
        self.dialect = self.engine.url.get_backend_name().lower()

    def _quote(self, name):
        if self.dialect in {"mysql", "mariadb"}:
            return f"`{name}`"
        if self.dialect in {"mssql"}:
            return f"[{name}]"
        return f'"{name}"'

    def _ensure_table(self, table_name, fields):
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

        cols = [pk]
        for name, value in fields.items():
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
            create_stmt = (
                f"IF OBJECT_ID(N'{table_name}', N'U') IS NULL "
                f"BEGIN CREATE TABLE {self._quote(table_name)} ({', '.join(cols)}); END;"
            )
        else:
            create_stmt = f"CREATE TABLE IF NOT EXISTS {self._quote(table_name)} ({', '.join(cols)});"

        print(f"Ensuring table -> {create_stmt}")

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
                return f"SQL Error: {exc}"

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
        return f"{self._quote(field)} {direction.upper()}"

    def _extract_limit(self, uql):
        match = re.search(r"LIMIT\s+(\d+)", uql, re.IGNORECASE)
        return match.group(1) if match else None

    def create(self, entity, data):
        fields = {str(k): str(v) for k, v in dict(data).items()}
        query = self.convert_uql(
            "CREATE " + str(entity) + " {" + ", ".join(f"{key}: {value}" for key, value in fields.items()) + "}"
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
        query = "FIND " + str(entity)
        if isinstance(where, dict) and where:
            parts = [f"{key} = {self._literal_to_uql(value)}" for key, value in where.items()]
            query += " WHERE " + " AND ".join(parts)
        elif isinstance(where, str) and where.strip():
            query += " WHERE " + where.strip()
        if order_by:
            query += " ORDER BY " + str(order_by)
        if limit is not None:
            query += " LIMIT " + str(limit)
        return self.run_native(self.convert_uql(query))

    def delete(self, entity, where):
        query = "DELETE " + str(entity)
        if isinstance(where, dict) and where:
            parts = [f"{key} = {self._literal_to_uql(value)}" for key, value in where.items()]
            query += " WHERE " + " AND ".join(parts)
        elif isinstance(where, str) and where.strip():
            query += " WHERE " + where.strip()
        return self.run_native(self.convert_uql(query))

    def update(self, entity, data, where):
        assignments = []
        params = {}
        for idx, (key, value) in enumerate(dict(data).items()):
            pname = f"u_{idx}"
            assignments.append(f"{self._quote(key)} = :{pname}")
            params[pname] = value
        where_sql = "1=1"
        where_params = {}
        if isinstance(where, dict) and where:
            where_sql, where_params = self._parameterize_condition(
                " AND ".join(f"{key} = {self._literal_to_uql(value)}" for key, value in where.items())
            )
        elif isinstance(where, str) and where.strip():
            where_sql, where_params = self._parameterize_condition(where.strip())
        sql = f"UPDATE {self._quote(entity)} SET {', '.join(assignments)} WHERE {where_sql}"  # nosec B608
        params.update(where_params)
        return self.run_native(ParameterizedSQL(sql, params))

    def count(self, entity, where=None):
        params = {}
        where_sql = ""
        if isinstance(where, dict) and where:
            where_sql, params = self._parameterize_condition(
                " AND ".join(f"{key} = {self._literal_to_uql(value)}" for key, value in where.items())
            )
        elif isinstance(where, str) and where.strip():
            where_sql, params = self._parameterize_condition(where.strip())
        sql = f"SELECT COUNT(*) AS total FROM {self._quote(entity)}"  # nosec B608
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
        if isinstance(value, bool):
            return "true" if value else "false"
        if value is None:
            return "null"
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"

    def _parameterize_condition(self, condition):
        condition = self._normalize_condition(condition)
        if not condition:
            return "", {}
        tokens = re.split(r"\s+(AND|OR)\s+", condition, flags=re.IGNORECASE)
        clauses = []
        params = {}
        value_index = 0
        for token in tokens:
            piece = token.strip()
            if not piece:
                continue
            upper = piece.upper()
            if upper in {"AND", "OR"}:
                clauses.append(upper)
                continue
            match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*(.+)", piece)
            if not match:
                raise ValueError("Unsupported legacy WHERE clause")
            field, op, raw = match.group(1), match.group(2), match.group(3)
            pname = f"w_{value_index}"
            value_index += 1
            clauses.append(f"{self._quote(field)} {op} :{pname}")
            params[pname] = self._parse_literal_value(raw)
        return " ".join(clauses), params

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
        value = str(raw).strip()
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            return value[1:-1]
        lowered = value.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        if re.fullmatch(r"-?\d+", value):
            return int(value)
        if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
            return float(value)
        return value
