"""
Query Builder with UModel Example

Demonstrates the fluent Query Builder API with UModel for typed results.
Uses Django-style model definitions with Column, CharField, IntegerField, etc.
"""

from DBDuck import UDOM
from DBDuck.models import (
    BooleanField,
    CharField,
    Column,
    ForeignKey,
    IntegerField,
    UModel,
    CASCADE,
    FloatField
)


# ─────────────────────────────────────────────────────────────────────────────
# Define Models using Django-style Column definitions
# ─────────────────────────────────────────────────────────────────────────────

class User(UModel):
    """User model with Django-style column definitions."""
    __entity__ = "users"
    
    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    email = Column(CharField, unique=True)
    role = Column(CharField, default="user")
    active = Column(BooleanField, default=True)
    age = Column(IntegerField, nullable=True)


class Order(UModel):
    """Order model with foreign key relationship."""
    __entity__ = "orders"
    
    id = Column(IntegerField, primary_key=True)
    user_id = ForeignKey(User, on_delete=CASCADE)
    amount = Column(FloatField)
    status = Column(CharField, default="pending")


# ─────────────────────────────────────────────────────────────────────────────
# Setup Database
# ─────────────────────────────────────────────────────────────────────────────

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
        status TEXT DEFAULT 'pending'
    )
""")

# Bind models to database
User.bind(db)
Order.bind(db)

# Insert test data using models
print("=== Creating Test Data with UModel ===")
User(id=1, name="Alice", email="alice@example.com", role="admin", active=1, age=30).save()
User(id=2, name="Bob", email="bob@example.com", role="user", active=1, age=25).save()
User(id=3, name="Charlie", email="charlie@example.com", role="user", active=0, age=35).save()
User(id=4, name="Diana", email="diana@example.com", role="admin", active=1, age=28).save()

Order(id=1, user_id=1, amount=100.00, status="completed").save()
Order(id=2, user_id=1, amount=50.00, status="pending").save()
Order(id=3, user_id=2, amount=75.50, status="completed").save()

print("Created 4 users and 3 orders\n")

# ─────────────────────────────────────────────────────────────────────────────
# Query Builder with Typed Results
# ─────────────────────────────────────────────────────────────────────────────

print("=== Query Builder Returns Typed Model Instances ===")

# find() returns list[User]
users = User.query().find()
print(f"Type of results: {type(users)}")
print(f"Type of first item: {type(users[0])}")
print(f"First user name (via attribute): {users[0].name}")
print(f"First user email (via attribute): {users[0].email}\n")

# first() returns User | None
user = User.query().where(id=1).first()
print(f"User.query().where(id=1).first() returns: {type(user).__name__}")
print(f"User details: id={user.id}, name={user.name}, role={user.role}\n")

# ─────────────────────────────────────────────────────────────────────────────
# Fluent Chaining
# ─────────────────────────────────────────────────────────────────────────────

print("=== Fluent Chaining ===")

# Chain multiple conditions
active_admins = (
    User.query()
    .where(active=1)
    .where(role="admin")
    .order("name")
    .find()
)
print(f"Active admins: {[u.name for u in active_admins]}")

# Using comparison operators
young_users = User.query().where_lt(age=30).find()
print(f"Users under 30: {[f'{u.name} ({u.age})' for u in young_users]}")

# ─────────────────────────────────────────────────────────────────────────────
# Count and Exists
# ─────────────────────────────────────────────────────────────────────────────

print("\n=== Count and Exists ===")

admin_count = User.query().where(role="admin").count()
print(f"Admin count: {admin_count}")

alice_exists = User.query().where(name="Alice").exists()
print(f"Alice exists: {alice_exists}")

nobody_exists = User.query().where(name="Nobody").exists()
print(f"Nobody exists: {nobody_exists}")

# ─────────────────────────────────────────────────────────────────────────────
# Clone for Reusable Queries
# ─────────────────────────────────────────────────────────────────────────────

print("\n=== Clone for Reusable Base Queries ===")

# Create reusable base query
active_users_base = User.query().where(active=1)

# Clone and extend
admins = active_users_base.clone().where(role="admin").find()
regular = active_users_base.clone().where(role="user").find()

print(f"Active admins: {[u.name for u in admins]}")
print(f"Active regular users: {[u.name for u in regular]}")

# ─────────────────────────────────────────────────────────────────────────────
# Pagination with Model Instances
# ─────────────────────────────────────────────────────────────────────────────

print("\n=== Pagination Returns Model Instances ===")

page = User.query().order("id").find_page(page=1, page_size=2)

print(f"Page: {page['page']}, Total: {page['total']}, Pages: {page['total_pages']}")
print(f"Items type: {type(page['items'][0]).__name__}")
print("Users on page 1:")
for user in page["items"]:
    print(f"  - {user.name} ({user.email})")

# ─────────────────────────────────────────────────────────────────────────────
# Updates via Query Builder
# ─────────────────────────────────────────────────────────────────────────────

print("\n=== Updates via Query Builder ===")

# Update using query builder
User.query().where(id=1).update({"name": "Alice Johnson"})

# Verify update
updated_user = User.query().where(id=1).first()
print(f"Updated user: {updated_user.name}")

# ─────────────────────────────────────────────────────────────────────────────
# Working with Model Methods
# ─────────────────────────────────────────────────────────────────────────────

print("\n=== Model Methods on Query Results ===")

user = User.query().where(id=2).first()

# Use model's to_dict() method
user_dict = user.to_dict()
print(f"to_dict(): {user_dict}")

# Modify and save
user.name = "Bob Smith"
user.update()

# Verify
updated = User.query().where(id=2).first()
print(f"After update: {updated.name}")

# ─────────────────────────────────────────────────────────────────────────────
# Cross-Model Queries
# ─────────────────────────────────────────────────────────────────────────────

print("\n=== Cross-Model Queries ===")

# Find orders for a specific user
alice = User.query().where(name="Alice Johnson").first()
alice_orders = Order.query().where(user_id=alice.id).find()
print(f"Alice's orders: {len(alice_orders)}")
for order in alice_orders:
    print(f"  - Order #{order.id}: ${order.amount} ({order.status})")

print("\n" + "=" * 50)
print("UModel Query Builder examples completed!")
print("=" * 50)
