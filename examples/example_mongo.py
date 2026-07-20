import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from DBDuck import UDOM

# Requires a running MongoDB server.
db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://127.0.0.1:27017")

print(db.execute("ping"))
# print(db.create("events", {"type": "login", "user": "username", "ok": True}))
print(db.find("events", where={"ok": True}))

# try:
#     with db.transaction():
#         # Internal-agi begin aagide
#         db.delete("events", where={'type': 'login'})
        
#         # Idu close aadaga automatic commit aagutthe
#     print("Delete success matthe commit aagide!")
# except Exception as e:
#     # Error bandre automatic rollback aagutthe
#     print(f"Transaction fail aagi rollback aagide: {e}")

# print(db.delete("events", where={"user": "username"}))
print(db.count("events"))
print(db.execute("show dbs"))
