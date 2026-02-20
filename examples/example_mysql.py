import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from DBDuck import UDOM

# Requires a running MySQL server and existing `udom` database.
db = UDOM(db_type="sql", db_instance="mysql", url="mysql+pymysql://root:pass@localhost:3306/dbduck")

print(db.create("Orders", {"order_id": 101, "customer": "A", "paid": True}))
print(db.find("Orders", where={"paid": True}, limit=10))
# print(db.delete("Orders", where={"order_id": 101}))
