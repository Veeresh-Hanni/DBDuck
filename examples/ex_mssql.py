import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from DBDuck import UDOM
from urllib.parse import quote_plus

# Requires a running SQL Server and existing `udom` database.
odbc = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=SERVERNAME;DATABASE=dbduck;Trusted_Connection=yes;"
url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc)

db = UDOM(db_type="sql", db_instance="mssql", url=url)

print(db.create("Orders", {"order_id": 101, "customer": "A", "paid": True}))
print(db.find("Orders", where={"paid": True}, limit=10))
