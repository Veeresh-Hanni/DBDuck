from udom.adapters.base_adapter import BaseAdapter
from sqlalchemy import create_engine, text
import re

class BaseSQLAdapter(BaseAdapter):
    def __init__(self, url):
        self.url = url
        self.engine = create_engine(url)

    # ---------- SQL runner ----------
    def run_native(self, query):
        print(f"🛠 Executing SQL → {query}")
        with self.engine.begin() as conn:
            try:
                result = conn.execute(text(query))
                try:
                    return result.fetchall()
                except:
                    return "Query executed successfully."
            except Exception as e:
                return f"SQL Error: {str(e)}"

    # ---------- Universal UQL → SQL ----------
    def convert_uql(self, uql):
        uql = uql.strip()

        if uql.startswith("FIND"):
            table, condition = self._extract_table_and_condition(uql)
            order_by = self._extract_order_by(uql)
            limit = self._extract_limit(uql)

            query = f"SELECT * FROM {self._quote(table)}"
            if condition:
                query += f" WHERE {condition}"
            if order_by:
                query += f" ORDER BY {order_by}"
            if limit:
                query += f" LIMIT {limit}"
            return query + ";"

        elif uql.startswith("CREATE"):
            table, fields = self._extract_table_and_body(uql)
            self._ensure_table(table, fields)
            columns = ", ".join([self._quote(c) for c in fields.keys()])
            values = ", ".join([self._format_value(v) for v in fields.values()])
            return f"INSERT INTO {self._quote(table)} ({columns}) VALUES ({values});"

        elif uql.startswith("DELETE"):
            table, condition = self._extract_table_and_condition(uql)
            return f"DELETE FROM {self._quote(table)} WHERE {condition};"

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
        return {k.strip(): v.strip() for k, v in (pair.split(":") for pair in fields.split(","))}

    # ---------- Abstract methods (override in child) ----------
    def _quote(self, name):
        raise NotImplementedError("Must implement in child adapters")

    def _format_value(self, val):
        raise NotImplementedError("Must implement in child adapters")

    def _ensure_table(self, table_name, fields):
        raise NotImplementedError("Must implement in child adapters")
