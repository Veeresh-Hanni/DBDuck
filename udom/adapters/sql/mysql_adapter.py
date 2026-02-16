from .base_sql_adapter import BaseSQLAdapter

class MySQLAdapter(BaseSQLAdapter):
    def _quote(self, name):
        return f"`{name}`"

    def _format_value(self, val):
        val = val.strip('"').strip("'")
        if val.lower() in ["true", "false"]:
            return "1" if val.lower() == "true" else "0"
        if val.isdigit():
            return val
        return f"'{val}'"

    def _ensure_table(self, table_name, fields):
        cols = [f"`id` INT PRIMARY KEY AUTO_INCREMENT"]
        for name, val in fields.items():
            if val.lower() in ["true", "false"]:
                cols.append(f"`{name}` BOOLEAN")
            elif val.isdigit():
                cols.append(f"`{name}` INT")
            else:
                cols.append(f"`{name}` VARCHAR(255)")

        create_stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols)});"
        self.run_native(create_stmt)
