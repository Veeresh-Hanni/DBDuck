import sys
from pathlib import Path
from urllib.parse import quote_plus

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from DBDuck import UDOM

odbc_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=server"
    "DATABASE=dbduck;"
    "UID=username;"
    "PWD=pass;"
    "TrustServerCertificate=yes;"
)
url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"

db = UDOM(db_type="sql", db_instance="mssql", url=url)

print(db.create("Orders", {"order_id": 101, "customer": "A", "paid": True}))
print(db.find("Orders", where={"paid": True}, limit=10))
