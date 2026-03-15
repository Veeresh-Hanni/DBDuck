import re
from typing import Any

from sqlalchemy import create_engine, text

from ..base_adapter import BaseAdapter


class ParameterizedSQL(str):
    def __new__(cls, sql: str, params: dict[str, Any] | None = None):
        obj = super().__new__(cls, sql)
        obj.params = params or {}
        return obj

class BaseSQLAdapter(BaseAdapter):
    def __init__(self, url):
        self.url = url
        self.engine = create_engine(url)

    # ---------- SQL runner ----------
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
            except Exception as e:
                return f"SQL Error: {str(e)}"

    # ---------- Universal UQL â†’ SQL ----------
    def convert_uql(self, uql):
        uql = uql.strip()
        cmd = uql.upper()

        if cmd.startswith("FIND"):
            table, condition = self._extract_table_and_condition(uql)
            order_by = self._extract_order_by(uql)
            limit = self._extract_limit(uql)

            query = f"SELECT * FROM {self._quote(table)}"  # nosec B608
            params = {}
            if condition:
                where_sql, params = self._parameterize_condition(condition)
                query += f" WHERE {where_sql}"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit:
                query += f" LIMIT {limit}"
            return ParameterizedSQL(query + ";", params)

        elif cmd.startswith("CREATE"):
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

        elif cmd.startswith("DELETE"):
            table, condition = self._extract_table_and_condition(uql)
            where_sql, params = self._parameterize_condition(condition or "1=1")
            sql = f"DELETE FROM {self._quote(table)} WHERE {where_sql};"  # nosec B608
            return ParameterizedSQL(sql, params)

        return "/* Unsupported UQL syntax */"

    # ---------- UQL Parsing Helpers ----------
    def _extract_table_and_condition(self, uql):
        """
        Extracts only the condition part (after WHERE), 
        stopping before ORDER BY or LIMIT.
        Supports AND / OR naturally.
        """
        match = re.match(
            r"(FIND|DELETE)\s+(\w+)"           # FIND User
            r"(?:\s+WHERE\s+(.+?))?"           # WHERE ...
            r"(?:\s+ORDER BY|\s+LIMIT|$)",     # Stop here
            uql,
            re.IGNORECASE
        )
        if not match:
            return None, None

        table = match.group(2)
        condition = match.group(3)

        # Ensure AND/OR spacing is clean
        if condition:
            condition = re.sub(r"\s+(AND|OR)\s+", r" \1 ", condition, flags=re.IGNORECASE)

        return table, condition


    def _extract_order_by(self, uql):
        match = re.search(r"ORDER BY\s+(\w+)\s*(ASC|DESC)?", uql, re.IGNORECASE)
        if match:
            field, direction = match.group(1), match.group(2) or "ASC"
            return f'{self._quote(field)} {direction}'
        return None

    def _extract_limit(self, uql):
        match = re.search(r"LIMIT\s+(\d+)", uql, re.IGNORECASE)
        return match.group(1) if match else None

    def _extract_table_and_body(self, uql):
        match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", uql, re.IGNORECASE)
        return match.group(1), self._parse_key_value_pairs(match.group(2))

    def _parse_key_value_pairs(self, fields):
        return {k.strip(): v.strip() for k, v in (pair.split(":", 1) for pair in fields.split(","))}

    def _parameterize_condition(self, condition):
        condition = (condition or "").strip()
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

    # ---------- Abstract methods (override in child) ----------
    def _quote(self, name):
        raise NotImplementedError("Must implement in child adapters")

    def _format_value(self, val):
        raise NotImplementedError("Must implement in child adapters")

    def _ensure_table(self, table_name, fields):
        raise NotImplementedError("Must implement in child adapters")
