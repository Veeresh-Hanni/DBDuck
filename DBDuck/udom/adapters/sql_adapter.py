from .base_adapter import BaseAdapter
from sqlalchemy import create_engine, text
import re


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

    def run_native(self, query):
        with self.engine.begin() as conn:
            try:
                result = conn.execute(text(query))
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

            if self.dialect == "mssql" and limit:
                query = f"SELECT TOP {limit} * FROM {self._quote(table)}"
            else:
                query = f"SELECT * FROM {self._quote(table)}"

            if condition:
                query += f" WHERE {self._normalize_condition(condition)}"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit and self.dialect != "mssql":
                query += f" LIMIT {limit}"
            return query + ";"

        if cmd.startswith("CREATE"):
            table, fields = self._extract_table_and_body(uql)
            self._ensure_table(table, fields)

            columns = ", ".join([self._quote(c) for c in fields.keys()])
            values = ", ".join([self._format_value(v) for v in fields.values()])
            return f"INSERT INTO {self._quote(table)} ({columns}) VALUES ({values});"

        if cmd.startswith("DELETE"):
            table, condition = self._extract_table_and_condition(uql)
            normalized = self._normalize_condition(condition) if condition else "1=1"
            return f"DELETE FROM {self._quote(table)} WHERE {normalized};"

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

    def _parse_key_value_pairs(self, fields):
        result = {}
        for pair in fields.split(","):
            key, val = pair.split(":", 1)
            result[key.strip()] = val.strip()
        return result

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
