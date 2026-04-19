from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import MetaData, Table

from DBDuck.cli.main import app

from ._helpers import env_value, require_env_flag, unique_entity


SQL_MIGRATION_CASES = [
    pytest.param(
        "sqlite",
        lambda project_dir: f"sqlite:///{(project_dir / 'migration_sqlite.db').as_posix()}",
        id="sqlite",
    ),
    pytest.param(
        "mysql",
        lambda _project_dir: env_value("MYSQL_TEST_URL", "mysql+pymysql://root:password@localhost:3306/udom_test"),
        id="mysql",
        marks=require_env_flag("RUN_MYSQL_INTEGRATION", reason="Set RUN_MYSQL_INTEGRATION=1 to run MySQL integration"),
    ),
    pytest.param(
        "postgres",
        lambda _project_dir: env_value(
            "POSTGRES_TEST_URL", "postgresql+psycopg2://postgres:password@localhost:5432/udom_test"
        ),
        id="postgres",
        marks=require_env_flag(
            "RUN_POSTGRES_INTEGRATION", reason="Set RUN_POSTGRES_INTEGRATION=1 to run PostgreSQL integration"
        ),
    ),
    pytest.param(
        "mssql",
        lambda _project_dir: env_value(
            "MSSQL_TEST_URL",
            "mssql+pyodbc://sa:Password!123@localhost:1433/udom_test?driver=ODBC+Driver+17+for+SQL+Server",
        ),
        id="mssql",
        marks=require_env_flag("RUN_MSSQL_INTEGRATION", reason="Set RUN_MSSQL_INTEGRATION=1 to run SQL Server integration"),
    ),
]


def _write_models_module(project_dir: Path, customer_table: str, order_table: str) -> None:
    (project_dir / "models.py").write_text(
        "\n".join(
            [
                "from DBDuck.models import Column, ForeignKey, Integer, String, UModel",
                "",
                "class Customer(UModel):",
                "    class Meta:",
                f"        db_table = {customer_table!r}",
                "",
                "    id = Column(Integer, primary_key=True)",
                "    name = Column(String, nullable=False)",
                "",
                "class Order(UModel):",
                "    class Meta:",
                f"        db_table = {order_table!r}",
                "",
                "    id = Column(Integer, primary_key=True)",
                "    customer_id = ForeignKey(Customer)",
                "    status = Column(String, nullable=False, default='pending')",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _drop_table_if_exists(engine, table_name: str) -> None:
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return
    table = Table(table_name, MetaData(), autoload_with=engine)
    table.drop(bind=engine, checkfirst=True)


def _cleanup_tables(url: str, table_names: list[str]) -> None:
    engine = create_engine(url)
    try:
        for table_name in ["alembic_version", *reversed(table_names)]:
            _drop_table_if_exists(engine, table_name)
    finally:
        engine.dispose()


@pytest.mark.parametrize(("db_instance", "url_factory"), SQL_MIGRATION_CASES)
def test_sql_backend_migration_flow_creates_project_local_workspace_and_tables(
    db_instance: str, url_factory, tmp_path: Path, monkeypatch
) -> None:
    project_dir = tmp_path / f"project_{db_instance}"
    project_dir.mkdir(parents=True, exist_ok=True)
    customer_table = unique_entity(f"it_{db_instance}_customers")
    order_table = unique_entity(f"it_{db_instance}_orders")
    _write_models_module(project_dir, customer_table, order_table)

    url = url_factory(project_dir)
    table_names = [customer_table, order_table]
    _cleanup_tables(url, table_names)

    monkeypatch.chdir(project_dir)
    monkeypatch.setenv("DATABASE_URL", url)

    try:
        assert app(["makemigrations", "--module", "models", "--message", "init"]) == 0
        assert app(["migrate", "--direction", "up"]) == 0

        migrations_dir = project_dir / "migrations" / "sql"
        assert (migrations_dir / "alembic.ini").exists()
        assert (migrations_dir / "env.py").exists()
        assert (migrations_dir / "versions").is_dir()
        revision_files = list((migrations_dir / "versions").glob("*.py"))
        assert revision_files

        engine = create_engine(url)
        try:
            inspector = inspect(engine)
            tables = set(inspector.get_table_names())
            assert customer_table in tables
            assert order_table in tables
            assert "alembic_version" in tables
        finally:
            engine.dispose()
    finally:
        _cleanup_tables(url, table_names)
