import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from DBDuck import UDOM

db = UDOM(
    db_type="sql",
    db_instance="postgres",
    # Use an existing database name in URL
    url="postgresql+psycopg2://postgres:pass@localhost:5432/dbduck",
)

print(db.create("Customer", {"name": "Veeresh", "age": 23, "active": True}))
print(db.find("Customer", where={"active": True}, limit=5))
# print(db.delete("Customer", where={"name": "Veeresh"}))
