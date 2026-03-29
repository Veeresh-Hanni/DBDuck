"""Database bootstrap and binding for the full stack DBDuck showcase app."""

from __future__ import annotations

from datetime import datetime
from typing import Type

from DBDuck import UDOM
from DBDuck.models import UModel

from ..core.config import get_env
from ..models import ALL_MODELS, Customer, Order, OrderItem, OrderTag, Product, Profile, Tag
from ..services.identity import normalize_email


def build_db() -> UDOM:
    return UDOM(
        url=get_env("APP_DB_URL", "postgresql+psycopg2://postgres:Veeru123@localhost:5432/fullstack_dbduck_app"),
        log_level=get_env("APP_LOG_LEVEL", "ERROR"),
    )


def bind_models(db: UDOM) -> None:
    for model in ALL_MODELS:
        model.bind(db)


def bootstrap_schema(db: UDOM) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            role TEXT DEFAULT 'customer'
        )
        """
    )
    try:
        db.execute("ALTER TABLE customers ADD COLUMN role TEXT DEFAULT 'customer'")
    except Exception:
        pass
    try:
        db.execute("UPDATE customers SET role = 'customer' WHERE role IS NULL")
    except Exception:
        pass
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            bio TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers (id)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            active INTEGER DEFAULT 1
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            paid INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            payment_provider TEXT DEFAULT '',
            payment_order_id TEXT DEFAULT '',
            payment_id TEXT DEFAULT '',
            FOREIGN KEY (customer_id) REFERENCES customers (id)
        )
        """
    )
    for statement in (
        "ALTER TABLE orders ADD COLUMN payment_provider TEXT DEFAULT ''",
        "ALTER TABLE orders ADD COLUMN payment_order_id TEXT DEFAULT ''",
        "ALTER TABLE orders ADD COLUMN payment_id TEXT DEFAULT ''",
    ):
        try:
            db.execute(statement)
        except Exception:
            pass
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS orderitems (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY (order_id) REFERENCES orders (id),
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS ordertags (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders (id),
            FOREIGN KEY (tag_id) REFERENCES tags (id)
        )
        """
    )


def next_id(model_cls: Type[UModel]) -> int:
    rows = model_cls.find(order_by="id DESC", limit=1)
    return int(rows[0].id) + 1 if rows else 1


def seed_demo_data() -> None:
    if Customer.count() > 0:
        existing_customers = Customer.query().order("id", "ASC").find()
        if not any(normalize_email(item.email.lower()) == "admin@dbduck.app" for item in existing_customers):
            admin_id = next_id(Customer)
            profile_id = next_id(Profile)
            Customer(
                id=admin_id,
                name="DBDuck Admin",
                email="admin@dbduck.app",
                password="admin123",
                active=True,
                role="admin",
            ).save()
            Profile(id=profile_id, customer_id=admin_id, bio="Admin account for the showcase app").save()
        return

    with Customer._udom.transaction():
        Customer(id=1, name="Asha", email="asha@example.com", password="secret123", active=True, role="customer").save()
        Profile(id=1, customer_id=1, bio="Loves clean APIs and SQLite demos").save()

        Customer(id=2, name="Ishan", email="ishan@example.com", password="secret123", active=True, role="customer").save()
        Profile(id=2, customer_id=2, bio="Builds internal tools with FastAPI").save()

        Customer(id=3, name="DBDuck Admin", email="admin@dbduck.app", password="admin123", active=True, role="admin").save()
        Profile(id=3, customer_id=3, bio="Admin account for the showcase app").save()

        Product(id=1, name="DBDuck Mug", price=299.0, active=True).save()
        Product(id=2, name="DBDuck Hoodie", price=1499.0, active=True).save()
        Product(id=3, name="DBDuck Stickers", price=99.0, active=True).save()

        Order(id=1, customer_id=1, paid=True, status="completed", created_at=datetime.utcnow().isoformat()).save()
        OrderItem(id=1, order_id=1, product_id=1, quantity=2).save()
        OrderItem(id=2, order_id=1, product_id=3, quantity=1).save()

        Order(id=2, customer_id=2, paid=False, status="pending", created_at=datetime.utcnow().isoformat()).save()
        OrderItem(id=3, order_id=2, product_id=2, quantity=1).save()

        Tag(id=1, name="priority").save()
        Tag(id=2, name="gift").save()
        OrderTag(id=1, order_id=1, tag_id=1).save()
        OrderTag(id=2, order_id=2, tag_id=2).save()
