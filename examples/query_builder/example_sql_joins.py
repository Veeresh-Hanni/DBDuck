"""
Query Builder SQL JOIN Example - SQLite

Demonstrates SQL-only join support in the fluent Query Builder API.
No external dependencies required - uses in-memory SQLite.
"""

from DBDuck import UDOM


def main() -> None:
    db = UDOM(url="sqlite:///:memory:")

    db.adapter.run_native(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT,
            active INTEGER DEFAULT 1
        )
        """
    )

    db.adapter.run_native(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            status TEXT
        )
        """
    )

    print("=== Inserting Test Data ===")
    db.table("users").create({"id": 1, "name": "Alice", "role": "admin", "active": 1})
    db.table("users").create({"id": 2, "name": "Bob", "role": "user", "active": 1})
    db.table("users").create({"id": 3, "name": "Charlie", "role": "user", "active": 0})
    db.table("users").create({"id": 4, "name": "Diana", "role": "admin", "active": 1})

    db.table("orders").create({"id": 101, "user_id": 1, "amount": 100.0, "status": "completed"})
    db.table("orders").create({"id": 102, "user_id": 1, "amount": 50.0, "status": "pending"})
    db.table("orders").create({"id": 103, "user_id": 2, "amount": 75.5, "status": "completed"})
    print("Inserted 4 users and 3 orders\n")

    print("=== INNER JOIN ===")
    joined_rows = (
        db.table("users")
        .join("orders", on=("id", "user_id"))
        .select("name", "orders.status", "orders.amount")
        .order_by("orders.id ASC")
        .find()
    )
    for row in joined_rows:
        print(row)

    print("\n=== JOIN WITH QUALIFIED WHERE ===")
    completed_orders = (
        db.table("users")
        .join("orders", on=("id", "user_id"))
        .where({"orders.status": "completed"})
        .select("name", "orders.amount")
        .order_by("orders.id ASC")
        .find()
    )
    print(completed_orders)

    print("\n=== LEFT JOIN ===")
    users_without_orders = (
        db.table("users")
        .left_join("orders", on=("id", "user_id"))
        .where({"orders.id__null": True})
        .select("name")
        .order("id", "ASC")
        .find()
    )
    print(users_without_orders)

    print("\n=== JOIN COUNT ===")
    completed_count = (
        db.table("users")
        .join("orders", on=("id", "user_id"))
        .where({"orders.status": "completed"})
        .count()
    )
    print(f"Completed joined rows: {completed_count}")

    print("\n=== JOIN FIRST ===")
    first_order = (
        db.table("users")
        .join("orders", on=("id", "user_id"))
        .select("name", "orders.amount")
        .order_by("orders.id ASC")
        .first()
    )
    print(first_order)

    print("\n" + "=" * 50)
    print("SQL JOIN Query Builder example completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
