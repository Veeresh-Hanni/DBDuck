import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from DBDuck import UDOM

# Requires a running MongoDB server.
db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/dbduck")

print(db.execute("ping"))
print(db.create("events", {"type": "login", "user": "veeresh", "ok": True}))
print(db.find("events", where={"ok": True}))
# print(db.delete("events", where={"user": "veeresh"}))
print(db.execute("show dbs"))
