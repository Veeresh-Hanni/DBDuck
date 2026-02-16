from .base_sql_adapter import BaseSQLAdapter

class MariaDBAdapter(BaseSQLAdapter):
    def __init__(self, url):
        # Example: mariadb+pymysql://user:pass@host/dbname
        super().__init__(url)

    def normalize_table_name(self, name):
        return name.lower()
