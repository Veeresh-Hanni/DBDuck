from DBDuck import UDOM

# Any SQL table name works (Customer, Product, Orders, etc.)
db = UDOM(db_type="sql", db_instance="sqlite", url="sqlite:///dbduck.db")

print(db.create("Product", {"name": "Keyboard", "price": 99, "active": True}))
print(db.create("Product", {"name": "Mouse", "price": 49, "active": True}))
print(db.find("Product", where={"active": True}, order_by="price DESC", limit=10))
print(db.create("Orders", {"order_id": 101, "customer": "A", "paid": True}))
print(db.find("Orders", where={"paid": True}, limit=10))

db.execute("DROP TABLE Orders")