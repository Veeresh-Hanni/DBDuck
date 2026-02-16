# udom/adapters/sql_adapter.py

from .base_adapter import BaseAdapter
from sqlalchemy import create_engine, text
import re

class SQLAdapter(BaseAdapter):
    def __init__(self, url="sqlite:///test.db"):
        """Create database connection using SQLAlchemy"""
        self.url = url
        self.engine = create_engine(url)

    def _ensure_table(self, table_name, fields):
        url = str(self.engine.url)

        if "sqlite" in url:
            pk = "INTEGER PRIMARY KEY AUTOINCREMENT"
            text_type = "TEXT"
            bool_type = "TEXT"
        elif "mysql" in url:
            pk = "INT PRIMARY KEY AUTO_INCREMENT"
            text_type = "VARCHAR(255)"
            bool_type = "BOOLEAN"
        elif "postgres" in url:
            pk = "SERIAL PRIMARY KEY"
            text_type = "TEXT"
            bool_type = "BOOLEAN"
        else:
            pk = "INT PRIMARY KEY"
            text_type = "TEXT"
            bool_type = "TEXT"

        cols = [f'id {pk}']
        for name, value in fields.items():
            if value.lower() in ["true", "false"]:
                cols.append(f'"{name}" {bool_type}')
            elif value.isdigit():
                cols.append(f'"{name}" INT')
            else:
                cols.append(f'"{name}" {text_type}')

        create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});'

        print(f"🛠 Ensuring table → {create_stmt}")

        with self.engine.begin() as conn:
            conn.execute(text(create_stmt))



    def run_native(self, query):
        """Execute raw SQL query on database"""
        with self.engine.begin() as conn:  # <-- this auto-commits
            result = conn.execute(text(query))

            try:
                return result.fetchall()  # Return rows
            except:
                return "Query executed successfully."

    def convert_uql(self, uql):
        """Convert simple UQL → SQL"""
        uql = uql.strip()

        # 🟢 FIND
        if uql.startswith("FIND"):
            table, condition = self._extract_table_and_condition(uql)
            return f"SELECT * FROM {table} WHERE {condition};" if condition else f"SELECT * FROM {table};"

        # 🟠 CREATE
        elif uql.startswith("CREATE"):
            table, fields = self._extract_table_and_body(uql)

            # 🆕 Auto-create table using fields from UModel
            self._ensure_table(table, fields)

            columns = ", ".join(fields.keys())
            values = ", ".join([self._format_value(v) for v in fields.values()])
            return f'INSERT INTO "{table}" ({columns}) VALUES ({values});'



        # 🔴 DELETE
        elif uql.startswith("DELETE"):
            table, condition = self._extract_table_and_condition(uql)
            return f"DELETE FROM {table} WHERE {condition};"

        return "/* Unsupported UQL syntax */"

    # 🔧 Helper Methods
    def _extract_table_and_condition(self, uql):
        match = re.match(r"(FIND|DELETE)\s+(\w+)(?:\s+WHERE\s+(.+))?", uql, re.IGNORECASE)
        return match.group(2), match.group(3)

    def _extract_table_and_body(self, uql):
        match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", uql, re.IGNORECASE)
        return match.group(1), self._parse_key_value_pairs(match.group(2))

    def _parse_key_value_pairs(self, fields):
        result = {}
        for pair in fields.split(","):
            key, val = pair.split(":")
            result[key.strip()] = val.strip()
        return result

    def _format_value(self, val):
        val = val.strip('"').strip("'")  # Remove wrapping quotes
        if val.isdigit():
            return val  # Number → no quotes
        return f"'{val}'"  # Wrap as SQL string

