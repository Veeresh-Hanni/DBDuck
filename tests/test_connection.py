from __future__ import annotations

from DBDuck.adapters.mssql_adapter import MSSQLAdapter
from DBDuck.adapters.sqlite_adapter import SQLiteAdapter
from DBDuck.core.adapter_router import AdapterRouter
from DBDuck.core.connection_manager import ConnectionManager


def test_parse_url_sqlite() -> None:
    parsed = ConnectionManager.parse_url("sqlite:///tmp_test.db")
    assert parsed.dialect == "sqlite"
    assert parsed.database == "tmp_test.db"


def test_engine_is_cached_per_url(tmp_path) -> None:
    db_path = tmp_path / "cache.db"
    url = f"sqlite:///{db_path.as_posix()}"
    manager = ConnectionManager()
    e1 = manager.get_engine(url)
    e2 = manager.get_engine(url)
    assert e1 is e2


def test_route_sql_adapter_supports_mssql_and_alias() -> None:
    resolved, adapter_cls = AdapterRouter.route_sql_adapter("mssql", None)
    assert resolved == "mssql"
    assert adapter_cls is MSSQLAdapter

    resolved, adapter_cls = AdapterRouter.route_sql_adapter(None, "sqlserver+pyodbc://username:pass@localhost/db")
    assert resolved == "mssql"
    assert adapter_cls is MSSQLAdapter


def test_route_sql_adapter_prefers_url_dialect_over_conflicting_db_instance() -> None:
    resolved, adapter_cls = AdapterRouter.route_sql_adapter("sqlite", "mssql+pyodbc://username:pass@localhost/db")
    assert resolved == "mssql"
    assert adapter_cls is MSSQLAdapter
    assert adapter_cls is not SQLiteAdapter
