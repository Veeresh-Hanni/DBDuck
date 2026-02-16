from .base_sql_adapter import BaseSQLAdapter

class PostgresAdapter(BaseSQLAdapter):
    def _quote(self, name):
        return f'"{name}"'

    def _format_value(self, val):
        val = val.strip('"').strip("'")
        if val.lower() in ["true", "false"]:
            return val.lower()
        if val.isdigit():
            return val
        return f"'{val}'"

    def _ensure_table(self, table_name, fields):
        cols = [f'"id" SERIAL PRIMARY KEY']
        for name, val in fields.items():
            if val.lower() in ["true", "false"]:
                cols.append(f'"{name}" BOOLEAN')
            elif val.isdigit():
                cols.append(f'"{name}" INT')
            else:
                cols.append(f'"{name}" TEXT')
        self.run_native(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(cols)});')

