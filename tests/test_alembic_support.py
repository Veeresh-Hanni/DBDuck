from __future__ import annotations

from sqlalchemy.dialects import mssql, mysql, postgresql, sqlite
from sqlalchemy.schema import CreateTable

import sqlalchemy as sa

from DBDuck.alembic_support import (
    apply_sqlalchemy_migration_compat,
    build_metadata_from_models,
    migration_context_options,
)
from DBDuck.models import Column, DateTimeField, ForeignKey, Integer, String, TextField, UModel
from examples.full_stack_dbduck_app.models import ALL_MODELS as SHOWCASE_MODELS


class AlembicCustomer(UModel):
    class Meta:
        db_table = "alembic_customers"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    notes = Column(TextField, nullable=True)


class AlembicOrder(UModel):
    class Meta:
        db_table = "alembic_orders"

    id = Column(Integer, primary_key=True)
    customer_id = ForeignKey(AlembicCustomer)
    status = Column(String, nullable=False)
    created_at = Column(DateTimeField, nullable=True)


def test_alembic_metadata_compiles_for_all_supported_sql_dialects() -> None:
    metadata = build_metadata_from_models([AlembicCustomer, AlembicOrder])

    dialects = {
        "sqlite": sqlite.dialect(),
        "mysql": mysql.dialect(),
        "postgresql": postgresql.dialect(),
        "mssql": mssql.dialect(),
    }

    for _name, dialect in dialects.items():
        for table_name in ("alembic_customers", "alembic_orders"):
            ddl = str(CreateTable(metadata.tables[table_name]).compile(dialect=dialect))
            assert "CREATE TABLE" in ddl


def test_alembic_metadata_uses_backend_safe_string_lengths_and_datetime() -> None:
    metadata = build_metadata_from_models([AlembicCustomer, AlembicOrder])

    mysql_customer = str(CreateTable(metadata.tables["alembic_customers"]).compile(dialect=mysql.dialect()))
    postgres_order = str(CreateTable(metadata.tables["alembic_orders"]).compile(dialect=postgresql.dialect()))
    mssql_order = str(CreateTable(metadata.tables["alembic_orders"]).compile(dialect=mssql.dialect()))

    assert "VARCHAR(255)" in mysql_customer
    assert "TEXT" in mysql_customer.upper()
    assert "TIMESTAMP" in postgres_order.upper() or "DATETIME" in postgres_order.upper()
    assert "DATETIME" in mssql_order.upper()


def test_mysql_migration_compat_adds_default_length_for_historical_sa_string() -> None:
    original_string = sa.String
    try:
        apply_sqlalchemy_migration_compat("mysql")
        assert sa.String().length == 255
        assert sa.String(64).length == 64
    finally:
        sa.String = original_string


def test_sqlite_migrations_enable_batch_alter_mode() -> None:
    assert migration_context_options("sqlite") == {"compare_type": True, "render_as_batch": True}
    assert migration_context_options("postgresql") == {"compare_type": True, "render_as_batch": False}


def test_datetime_field_empty_string_default_is_not_emitted_as_mysql_server_default() -> None:
    class AuditEvent(UModel):
        class Meta:
            db_table = "audit_events"

        id = Column(Integer, primary_key=True)
        created_at = Column(DateTimeField, nullable=False, default="")

    metadata = build_metadata_from_models([AuditEvent])
    ddl = str(CreateTable(metadata.tables["audit_events"]).compile(dialect=mysql.dialect()))
    assert "DEFAULT ''" not in ddl


def test_showcase_order_model_compiles_without_backend_specific_datetime_defaults() -> None:
    metadata = build_metadata_from_models(SHOWCASE_MODELS)
    mysql_ddl = str(CreateTable(metadata.tables["orders"]).compile(dialect=mysql.dialect()))
    postgres_ddl = str(CreateTable(metadata.tables["orders"]).compile(dialect=postgresql.dialect()))
    mssql_ddl = str(CreateTable(metadata.tables["orders"]).compile(dialect=mssql.dialect()))

    assert "created_at DATETIME NOT NULL DEFAULT ''" not in mysql_ddl
    assert "paid BOOL NOT NULL DEFAULT false" in mysql_ddl
    assert "CREATE TABLE" in postgres_ddl
    assert "paid BOOLEAN DEFAULT false NOT NULL" in postgres_ddl
    assert "CREATE TABLE" in mssql_ddl
    assert "paid BIT NOT NULL DEFAULT 0" in mssql_ddl


def test_boolean_defaults_compile_portably_for_postgres_and_mysql() -> None:
    class FlaggedOrder(UModel):
        class Meta:
            db_table = "flagged_orders"

        id = Column(Integer, primary_key=True)
        paid = Column(sa.Boolean, nullable=False, default=False)
        active = Column(sa.Boolean, nullable=False, default=True)

    metadata = build_metadata_from_models([FlaggedOrder])
    postgres_ddl = str(CreateTable(metadata.tables["flagged_orders"]).compile(dialect=postgresql.dialect()))
    mysql_ddl = str(CreateTable(metadata.tables["flagged_orders"]).compile(dialect=mysql.dialect()))

    assert "BOOLEAN DEFAULT 0" not in postgres_ddl.upper()
    assert "BOOL DEFAULT 0" not in postgres_ddl.upper()
    assert "DEFAULT false".upper() in postgres_ddl.upper()
    assert "DEFAULT true".upper() in postgres_ddl.upper()
    assert "CREATE TABLE" in mysql_ddl


def test_alembic_metadata_accepts_mongo_style_index_specs() -> None:
    class IndexedEvent(UModel):
        __entity__ = "indexed_events"
        __indexes__ = [
            {"fields": [{"name": "account_id"}, {"name": "created_at", "order": "desc"}]},
        ]

        account_id: int
        created_at: str

    metadata = build_metadata_from_models([IndexedEvent])
    indexes = {index.name: [column.name for column in index.columns] for index in metadata.tables["indexed_events"].indexes}

    assert indexes == {"ix_indexed_events_account_id_created_at": ["account_id", "created_at"]}
