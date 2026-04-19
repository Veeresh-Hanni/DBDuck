from __future__ import annotations

from DBDuck import UDOM
from DBDuck.models import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    ManyToMany,
    ManyToOne,
    OneToMany,
    OneToOne,
    String,
    UModel,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from DBDuck.alembic_support import build_metadata_from_models

class Order(UModel):
    class Meta:
        db_table = "orders_django_style"

    order_id = Column(Integer, primary_key=True)
    customer = Column(String, nullable=False)
    paid = Column(Boolean, default=False)


class Customer(UModel):
    class Meta:
        db_table = "customers_django_style"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)


class Invoice(UModel):
    class Meta:
        db_table = "invoices_django_style"

    id = Column(Integer, primary_key=True)
    customer_id = ForeignKey(Customer)
    amount = Column(Integer, nullable=False)
    customer = ManyToOne(Customer, fk_field="customer_id")


class Profile(UModel):
    class Meta:
        db_table = "profiles_django_style"

    id = Column(Integer, primary_key=True)
    customer_id = ForeignKey(Customer)
    bio = Column(String, nullable=False)


class Tag(UModel):
    class Meta:
        db_table = "tags_django_style"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)


class InvoiceTag(UModel):
    class Meta:
        db_table = "invoice_tags_django_style"

    id = Column(Integer, primary_key=True)
    invoice_id = Column(Integer, nullable=False)
    tag_id = Column(Integer, nullable=False)


Customer.profile = OneToOne(Profile, foreign_key="customer_id", local_key="id")
Customer.invoices = OneToMany(Invoice, foreign_key="customer_id", local_key="id")
Invoice.tags = ManyToMany(Tag, through=InvoiceTag, from_key="invoice_id", to_key="tag_id")


def test_dbduck_models_django_style_sqlite_crud(tmp_path) -> None:
    db_file = tmp_path / "dbduck_models_compat.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    Order.bind(db)

    created = Order(order_id=1, customer="Alice").save()
    assert created["rows_affected"] == 1

    rows = Order.find(where={"customer": "Alice"}, limit=1)
    assert len(rows) == 1
    assert rows[0].paid is False

    updated = rows[0].update(data={"paid": True}, where={"order_id": 1})
    assert updated["rows_affected"] == 1

    fetched = Order.find_one(where={"order_id": 1})
    assert fetched is not None
    assert fetched.customer == "Alice"
    assert fetched.paid is True

    deleted = fetched.delete(where={"order_id": 1})
    assert deleted["rows_affected"] == 1


def test_dbduck_models_foreign_key_accepts_model_instance(tmp_path) -> None:
    db_file = tmp_path / "dbduck_models_fk.db"
    db = UDOM(db_type="sql", db_instance="mysql", url=f"sqlite:///{db_file.as_posix()}")
    Customer.bind(db)
    Invoice.bind(db)

    Customer(id=1, name="Alice").save()
    alice = Customer.find_one(where={"id": 1})
    assert alice is not None

    created = Invoice(id=10, customer_id=alice, amount=500).save()
    assert created["rows_affected"] == 1

    rows = Invoice.find(where={"customer_id": 1}, limit=1)
    assert len(rows) == 1
    assert rows[0].amount == 500
    assert rows[0].customer is not None
    assert rows[0].customer.name == "Alice"


def test_dbduck_models_relations_one_to_one_one_to_many_many_to_many(tmp_path) -> None:
    db_file = tmp_path / "dbduck_models_relations.db"
    db = UDOM(db_type="sql", db_instance="mysql", url=f"sqlite:///{db_file.as_posix()}")
    Customer.bind(db)
    Profile.bind(db)
    Invoice.bind(db)
    Tag.bind(db)
    InvoiceTag.bind(db)

    Customer(id=1, name="Alice").save()
    Profile(id=11, customer_id=1, bio="First customer").save()
    Invoice(id=100, customer_id=1, amount=250).save()
    Invoice(id=101, customer_id=1, amount=350).save()
    Tag(id=1, name="urgent").save()
    Tag(id=2, name="paid").save()
    InvoiceTag(id=1001, invoice_id=100, tag_id=1).save()
    InvoiceTag(id=1002, invoice_id=100, tag_id=2).save()

    alice = Customer.find_one(where={"id": 1})
    assert alice is not None
    assert alice.profile is not None
    assert alice.profile.bio == "First customer"

    invoices = alice.invoices
    assert len(invoices) == 2
    amounts = sorted(i.amount for i in invoices)
    assert amounts == [250, 350]

    inv = Invoice.find_one(where={"id": 100})
    assert inv is not None
    tags = inv.tags
    assert len(tags) == 2
    assert sorted(t.name for t in tags) == ["paid", "urgent"]


def test_dbduck_models_create_table_and_migration_history(tmp_path) -> None:
    db_file = tmp_path / "dbduck_models_migrations.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")

    class UserV1(UModel):
        class Meta:
            db_table = "users"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        role = Column(String, default="user")
        active = Column(Boolean, default=False)

    UserV1.bind(db)
    created = UserV1.create_table()
    assert created["table"] == "users"

    UserV1(id=1, name="Asha", role="admin", active=True).save()
    history = UserV1.migration_history()
    assert len(history) == 1
    assert history[0]["operation"] == "create_table"


def test_dbduck_models_migrate_adds_missing_columns_without_recreating_db(tmp_path) -> None:
    db_file = tmp_path / "dbduck_models_add_column.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")

    class UserV1(UModel):
        class Meta:
            db_table = "users"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        active = Column(Boolean, default=False)

    class UserV2(UModel):
        class Meta:
            db_table = "users"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        active = Column(Boolean, default=False)
        age = Column(Integer, nullable=True)

    UserV1.bind(db)
    UserV1.create_table()
    UserV1(id=1, name="Asha", active=True).save()

    UserV2.bind(db)
    migrated = UserV2.migrate()
    assert migrated["created"] is False
    assert "age" in migrated["added_columns"]

    UserV2(id=2, name="Mira", active=False, age=29).save()
    fetched = UserV2.find_one(where={"id": 2})
    assert fetched is not None
    assert fetched.age == 29

    history = UserV2.migration_history()
    operations = [row["operation"] for row in history]
    assert "create_table" in operations
    assert "add_column" in operations


def test_udom_migrate_models_runs_multiple_model_migrations(tmp_path) -> None:
    db_file = tmp_path / "dbduck_migrate_models.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")

    class User(UModel):
        class Meta:
            db_table = "users"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)

    class Team(UModel):
        class Meta:
            db_table = "teams"

        id = Column(Integer, primary_key=True)
        title = Column(String, nullable=False)

    results = db.migrate_models(User, Team)
    tables = {item["table"] for item in results}
    assert tables == {"users", "teams"}


def test_dbduck_models_migrate_adds_non_nullable_column_when_default_exists(tmp_path) -> None:
    db_file = tmp_path / "dbduck_models_add_defaulted_column.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")

    class UserV1(UModel):
        class Meta:
            db_table = "users_defaulted"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)

    class UserV2(UModel):
        class Meta:
            db_table = "users_defaulted"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        role = Column(String, nullable=False, default="user")

    UserV1.bind(db)
    UserV1.create_table()
    UserV1(id=1, name="Asha").save()

    UserV2.bind(db)
    migrated = UserV2.migrate()
    assert migrated["created"] is False
    assert "role" in migrated["added_columns"]

    fetched_existing = UserV2.find_one(where={"id": 1})
    assert fetched_existing is not None
    assert fetched_existing.role == "user"

    UserV2(id=2, name="Mira").save()
    fetched_new = UserV2.find_one(where={"id": 2})
    assert fetched_new is not None
    assert fetched_new.role == "user"


def test_alembic_metadata_uses_mysql_safe_varchar_lengths() -> None:
    class Customer(UModel):
        class Meta:
            db_table = "customers_mysql_safe"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)

    metadata = build_metadata_from_models([Customer])
    ddl = str(CreateTable(metadata.tables["customers_mysql_safe"]).compile(dialect=mysql.dialect()))
    assert "VARCHAR(255)" in ddl


def test_dbduck_models_manager_get_or_create_and_update_or_create(tmp_path) -> None:
    db_file = tmp_path / "dbduck_models_manager.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")

    class User(UModel):
        class Meta:
            db_table = "users_manager"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        role = Column(String, nullable=False, default="user")

    User.bind(db)
    User.migrate()

    created, was_created = User.objects.get_or_create(id=1, defaults={"name": "Asha"})
    assert was_created is True
    assert created.name == "Asha"
    assert created.role == "user"

    same, was_created = User.objects.get_or_create(id=1, defaults={"name": "Ignored"})
    assert was_created is False
    assert same.id == 1

    updated, was_created = User.objects.update_or_create(id=1, defaults={"name": "Mira", "role": "admin"})
    assert was_created is False
    assert updated.name == "Mira"
    assert updated.role == "admin"

    fetched = User.objects.get(id=1)
    assert fetched.name == "Mira"
    assert fetched.role == "admin"


def test_dbduck_models_save_updates_existing_row_by_primary_key(tmp_path) -> None:
    db_file = tmp_path / "dbduck_models_save_update.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")

    class User(UModel):
        class Meta:
            db_table = "users_save_update"

        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        role = Column(String, nullable=False, default="user")

    User.bind(db)
    User.migrate()

    user = User(id=1, name="Asha")
    user.save()
    user.name = "Asha Updated"
    user.role = "admin"
    result = user.save()

    assert result["rows_affected"] == 1
    assert User.objects.count() == 1

    fetched = User.objects.get(id=1)
    assert fetched.name == "Asha Updated"
    assert fetched.role == "admin"

    fetched.name = "Asha Final"
    fetched.refresh_from_db()
    assert fetched.name == "Asha Updated"
