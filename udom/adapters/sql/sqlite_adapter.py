from .base_sql_adapter import BaseSQLAdapter

class SQLiteAdapter(BaseSQLAdapter):
    def _quote(self, name):
        return f'"{name}"'

    def _format_value(self, val):
        val = val.strip('"').strip("'")
        if val.lower() in ["true", "false"]:
            return "1" if val.lower() == "true" else "0"
        if val.isdigit():
            return val
        return f"'{val}'"

    def _ensure_table(self, table_name, fields):
        cols = [f'"id" INTEGER PRIMARY KEY AUTOINCREMENT']
        for name, val in fields.items():
            cols.append(f'"{name}" TEXT')
        self.run_native(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});')

