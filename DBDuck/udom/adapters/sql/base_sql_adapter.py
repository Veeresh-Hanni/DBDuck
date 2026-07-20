import re

from sqlalchemy import create_engine, text

from ....core.exceptions import QueryError
from ....utils.logger import get_logger, log_internal_debug
from ..base_adapter import BaseAdapter
from ._legacy_sql_common import ParameterizedSQL, parameterize_condition, parse_literal_value

class BaseSQLAdapter(BaseAdapter):
    def __init__(self, url):
        self.url = url
        self.engine = create_engine(url)
        self._logger = get_logger()

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
                log_internal_debug(
                    self._logger,
                    "Legacy BaseSQLAdapter execution failed",
                    event="query.error.internal",
                    db="legacy-sql",
                    exc=e,
                )
                raise QueryError(str(e) or e.__class__.__name__) from e

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
        return parameterize_condition(condition, quote_identifier=self._quote)

    @staticmethod
    def _parse_literal_value(raw):
        return parse_literal_value(raw)

    # ---------- Abstract methods (override in child) ----------
    def _quote(self, name):
        raise NotImplementedError("Must implement in child adapters")

    def _format_value(self, val):
        raise NotImplementedError("Must implement in child adapters")

    def _ensure_table(self, table_name, fields):
        raise NotImplementedError("Must implement in child adapters")
