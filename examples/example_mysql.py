import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from DBDuck import UDOM

# Requires a running MySQL server and existing `dbduck` database.
db = UDOM(db_type="sql", db_instance="mysql", url="mysql+pymysql://username:pass@localhost:3306/dbduck")

print(db.create("Orders", {"order_id": 101, "customer": "A", "paid": True}))
# with db.transaction():
#     db.create_many(
#         "Orders",
#         [
#             {"order_id": 102, "customer": "B", "paid": False},
#             {"order_id": 103, "customer": "C", "paid": True},
#         ],
#     )
print(db.find("Orders", where={"paid": True}, limit=10))
