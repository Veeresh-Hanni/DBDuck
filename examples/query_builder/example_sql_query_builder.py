"""
Query Builder DSL Example - SQL (SQLite)

Demonstrates the fluent Query Builder API with SQLite database.
No external dependencies required - uses in-memory SQLite.
"""

from DBDuck import UDOM, QueryBuilder

# Create in-memory SQLite database
db = UDOM(url="sqlite:///:memory:")

# Create tables
db.adapter.run_native("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        role TEXT DEFAULT 'user',
        active INTEGER DEFAULT 1,
        age INTEGER
    )
""")

db.adapter.run_native("""
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        amount REAL,
        status TEXT DEFAULT 'pending',
        created_at TEXT
    )
""")

# Insert test data
print("=== Inserting Test Data ===")
db.table("users").create({"id": 1, "name": "Alice", "email": "alice@example.com", "role": "admin", "active": 1, "age": 30})
db.table("users").create({"id": 2, "name": "Bob", "email": "bob@example.com", "role": "user", "active": 1, "age": 25})
db.table("users").create({"id": 3, "name": "Charlie", "email": "charlie@example.com", "role": "user", "active": 0, "age": 35})
db.table("users").create({"id": 4, "name": "Diana", "email": "diana@example.com", "role": "admin", "active": 1, "age": 28})
db.table("users").create({"id": 5, "name": "Eve", "email": "eve@example.com", "role": "user", "active": 1, "age": 22})

db.table("orders").create({"id": 1, "user_id": 1, "amount": 100.00, "status": "completed"})
db.table("orders").create({"id": 2, "user_id": 1, "amount": 50.00, "status": "pending"})
db.table("orders").create({"id": 3, "user_id": 2, "amount": 75.50, "status": "completed"})
db.table("orders").create({"id": 4, "user_id": 3, "amount": 200.00, "status": "cancelled"})

print("Inserted 5 users and 4 orders\n")

# ─────────────────────────────────────────────────────────────────────────────
# Basic Queries
# ─────────────────────────────────────────────────────────────────────────────
print("=== Basic Queries ===")

# Find all users
all_users = db.table("users").find()
print(f"All users: {len(all_users)}")

# Find with where condition
active_users = db.table("users").where(active=1).find()
print(f"Active users: {len(active_users)}")

# Find first matching record
admin = db.table("users").where(role="admin").first()
print(f"First admin: {admin['name']}")

# Count records
admin_count = db.table("users").where(role="admin").count()
print(f"Admin count: {admin_count}")

# Check existence
has_alice = db.table("users").where(name="Alice").exists()
print(f"Alice exists: {has_alice}\n")

# ─────────────────────────────────────────────────────────────────────────────
# Chaining & Ordering
# ─────────────────────────────────────────────────────────────────────────────
print("=== Chaining & Ordering ===")

# Order by name ascending
ordered = db.table("users").where(active=1).order("name", "ASC").find()
print(f"Active users (A-Z): {[u['name'] for u in ordered]}")

# Order by age descending, limit 3
oldest = db.table("users").order("age", "DESC").limit(3).find()
print("3 oldest: " + str([f"{u['name']} ({u['age']})" for u in oldest]))

# Full chain
result = (
    db.table("users")
    .where(active=1)
    .where(role="user")
    .order("age")
    .limit(2)
    .find()
)
print(f"2 youngest active users: {[u['name'] for u in result]}\n")

# ─────────────────────────────────────────────────────────────────────────────
# Field Projection
# ─────────────────────────────────────────────────────────────────────────────
print("=== Field Projection ===")

# Select specific fields
projected = db.table("users").select("name", "email").limit(2).find()
print(f"Projected fields: {projected}")

# ─────────────────────────────────────────────────────────────────────────────
# Comparison Operators (Note: These produce structured where conditions)
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== Comparison Operators (Query State) ===")

# Note: where_gt, where_lt etc. build structured conditions
# Full operator support depends on the backend adapter
# Here we show how to build the query state

query = db.table("users").where_gt(age=25)
print(f"where_gt query state: {query.to_dict()['where']}")

query2 = db.table("users").where_gte(age=25).where_lte(age=35)
print(f"Range query state: {query2.to_dict()['where']}")

# ─────────────────────────────────────────────────────────────────────────────
# Pagination
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== Pagination ===")

# Using page() method
page1 = db.table("users").order("id").page(1, 2).find()
page2 = db.table("users").order("id").page(2, 2).find()
print(f"Page 1: {[u['name'] for u in page1]}")
print(f"Page 2: {[u['name'] for u in page2]}")

# Using find_page() for full pagination info
page_data = db.table("users").where(active=1).find_page(page=1, page_size=2)
print(f"Paginated: page={page_data['page']}, total={page_data['total']}, pages={page_data['total_pages']}")

# ─────────────────────────────────────────────────────────────────────────────
# Clone for Reusable Queries
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== Clone for Reuse ===")

base_query = db.table("users").where(active=1)

admins = base_query.clone().where(role="admin").find()
regular = base_query.clone().where(role="user").find()

print(f"Active admins: {[u['name'] for u in admins]}")
print(f"Active users: {[u['name'] for u in regular]}")

# ─────────────────────────────────────────────────────────────────────────────
# Updates & Deletes
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== Updates & Deletes ===")

# Update a record
db.table("users").where(id=1).update({"name": "Alice Smith"})
updated = db.table("users").where(id=1).first()
print(f"Updated user: {updated['name']}")

# Delete a record
db.table("users").where(id=5).delete()
remaining = db.table("users").count()
print(f"Users after delete: {remaining}")

# ─────────────────────────────────────────────────────────────────────────────
# Query Introspection
# ─────────────────────────────────────────────────────────────────────────────
print("\n=== Query Introspection ===")

query = db.table("users").where(active=1).order("name").limit(10)
state = query.to_dict()
print(f"Query state: {state}")

print("\n" + "=" * 50)
print("All Query Builder examples completed successfully!")
print("=" * 50)
