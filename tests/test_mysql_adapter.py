from __future__ import annotations

import pytest
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from DBDuck.adapters.mysql_adapter import MySQLAdapter
from DBDuck.adapters.sqlite_adapter import SQLiteAdapter
from DBDuck.core.exceptions import ConnectionError, QueryError


def test_mysql_adapter_identifier_quoting() -> None:
    adapter = MySQLAdapter(url="sqlite:///:memory:")
    assert adapter._quote("orders") == "`orders`"


def test_sqlite_adapter_create_find_delete_roundtrip(tmp_path) -> None:
    db_path = tmp_path / "adapter_roundtrip.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")

    create_result = adapter.create("Orders", {"order_id": 101, "customer": "A", "paid": True})
    assert create_result["rows_affected"] == 1

    rows = adapter.find("Orders", where={"paid": True})
    assert len(rows) == 1
    assert rows[0]["order_id"] == 101

    delete_result = adapter.delete("Orders", where={"order_id": 101})
    assert delete_result["rows_affected"] == 1


def test_sqlite_adapter_create_many(tmp_path) -> None:
    db_path = tmp_path / "adapter_batch.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")
    result = adapter.create_many(
        "Orders",
        [
            {"order_id": 201, "customer": "A", "paid": True},
            {"order_id": 202, "customer": "B", "paid": False},
        ],
    )
    assert result["rows_affected"] == 2


def test_sqlite_adapter_rejects_invalid_numeric_where_value(tmp_path) -> None:
    db_path = tmp_path / "adapter_numeric_guard.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")
    adapter.create("users", {"id": 1, "name": "basu"})

    with pytest.raises(QueryError, match="Invalid integer value for field 'id'"):
        adapter.find("users", where={"id": "1 OR 1=1"})


def test_sqlite_adapter_allows_sql_like_text_in_string_field(tmp_path) -> None:
    db_path = tmp_path / "adapter_string_literal.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")
    adapter.create("users", {"id": 1, "name": "Veeresh'; DROP TABLE users; --"})

    rows = adapter.find("users", where={"name": "Veeresh'; DROP TABLE users; --"})
    assert len(rows) == 1
    assert rows[0]["name"] == "Veeresh'; DROP TABLE users; --"


def test_run_native_strips_sqlalchemy_background_link(tmp_path) -> None:
    db_path = tmp_path / "adapter_error.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")

    def _raise_sqlalchemy_error(*args, **kwargs):
        raise SQLAlchemyError(
            '(pymysql.err.OperationalError) (1049, "Unknown database \'dbduck\'")\n'
            "(Background on this error at: https://sqlalche.me/e/20/e3q8)"
        )

    class _Conn:
        def execute(self, *_args, **_kwargs):
            _raise_sqlalchemy_error()

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]

    with pytest.raises(QueryError) as excinfo:
        adapter.run_native("SELECT 1")

    message = str(excinfo.value)
    assert message == "(pymysql.err.OperationalError) (1049, \"Unknown database 'dbduck'\")"


def test_run_native_non_select_fetch_failure_falls_back_to_rows_affected(tmp_path) -> None:
    db_path = tmp_path / "adapter_fetch_fallback.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")

    class _BadMappings:
        def all(self):
            raise RuntimeError("driver fetch failure")

    class _BadResult:
        returns_rows = True
        rowcount = 0

        def mappings(self):
            return _BadMappings()

    class _Conn:
        def execute(self, *_args, **_kwargs):
            return _BadResult()

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]
    result = adapter.run_native("CREATE TABLE x (id INT)")
    assert result == {"rows_affected": 0}


def test_run_native_closes_result_object(tmp_path) -> None:
    db_path = tmp_path / "adapter_result_close.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")
    state = {"closed": False}

    class _Rows:
        def all(self):
            return [{"id": 1}]

    class _Result:
        returns_rows = True
        rowcount = 1

        def mappings(self):
            return _Rows()

        def close(self):
            state["closed"] = True

    class _Conn:
        def execute(self, *_args, **_kwargs):
            return _Result()

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]
    rows = adapter.run_native("SELECT 1")
    assert rows == [{"id": 1}]
    assert state["closed"] is True


def test_run_native_consumes_rows_before_context_exit(tmp_path) -> None:
    db_path = tmp_path / "adapter_context_fetch.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")
    state = {"closed": False}

    class _Rows:
        def all(self):
            if state["closed"]:
                raise RuntimeError("fetched after context exit")
            return [{"id": 1}]

    class _Result:
        returns_rows = True
        rowcount = 1

        def mappings(self):
            return _Rows()

        def close(self):
            pass

    class _Conn:
        def execute(self, *_args, **_kwargs):
            return _Result()

    class _BeginCtx:
        def __enter__(self):
            return _Conn()

        def __exit__(self, exc_type, exc, tb):
            state["closed"] = True

    class _Engine:
        def begin(self):
            return _BeginCtx()

    adapter._active_connection = lambda: None  # type: ignore[method-assign]
    adapter.engine = _Engine()  # type: ignore[assignment]

    rows = adapter.run_native("SELECT 1")
    assert rows == [{"id": 1}]


def test_convert_uql_rejects_where_injection() -> None:
    adapter = MySQLAdapter(url="sqlite:///:memory:")
    with pytest.raises(QueryError, match="Potential SQL injection"):
        adapter.convert_uql("FIND Orders WHERE paid = true; DROP TABLE Orders")


def test_convert_uql_rejects_invalid_order_by() -> None:
    adapter = MySQLAdapter(url="sqlite:///:memory:")
    with pytest.raises(QueryError, match="Invalid order_by clause"):
        adapter.convert_uql("FIND Orders ORDER BY order_id DESC, customer ASC")


def test_run_native_maps_connection_refused_to_connection_error(tmp_path) -> None:
    db_path = tmp_path / "adapter_conn_error.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")

    class _DBAPIOrig(Exception):
        def __init__(self):
            super().__init__(2003, "Can't connect to MySQL server on 'localhost' ([Errno 111] Connection refused)")
            self.args = (2003, "Can't connect to MySQL server on 'localhost' ([Errno 111] Connection refused)")

    class _Conn:
        def execute(self, *_args, **_kwargs):
            raise OperationalError("SELECT 1", {}, _DBAPIOrig())

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]

    with pytest.raises(ConnectionError) as excinfo:
        adapter.run_native("SELECT 1")
    assert "Can't connect to MySQL server on 'localhost'" in str(excinfo.value)


def test_run_native_maps_sqlstate_connection_error_to_connection_error(tmp_path) -> None:
    db_path = tmp_path / "adapter_conn_error_sqlstate.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")

    class _DBAPIOrig(Exception):
        def __init__(self):
            super().__init__("08001", "could not connect to server")
            self.args = ("08001", "could not connect to server")

    class _Conn:
        def execute(self, *_args, **_kwargs):
            raise OperationalError("SELECT 1", {}, _DBAPIOrig())

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]

    with pytest.raises(ConnectionError):
        adapter.run_native("SELECT 1")


def test_run_native_keeps_query_error_for_non_connection_operational_error(tmp_path) -> None:
    db_path = tmp_path / "adapter_query_error.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")

    class _DBAPIOrig(Exception):
        def __init__(self):
            super().__init__(1064, "You have an error in your SQL syntax")
            self.args = (1064, "You have an error in your SQL syntax")

    class _Conn:
        def execute(self, *_args, **_kwargs):
            raise OperationalError("SELECT * FROM", {}, _DBAPIOrig())

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]

    with pytest.raises(QueryError):
        adapter.run_native("SELECT * FROM")


def test_run_native_maps_raw_connection_like_exception_to_connection_error(tmp_path) -> None:
    db_path = tmp_path / "adapter_raw_conn_error.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")

    class _Conn:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("connection refused")

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):
        adapter.run_native("SELECT 1")


def test_run_native_maps_raw_non_connection_exception_to_query_error(tmp_path) -> None:
    db_path = tmp_path / "adapter_raw_query_error.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")

    class _Conn:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("invalid sql syntax")

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]
    with pytest.raises(QueryError):
        adapter.run_native("SELECT 1")


def test_sqlite_adapter_aggregate_group_by_metrics_and_having(tmp_path) -> None:
    db_path = tmp_path / "adapter_aggregate.db"
    adapter = SQLiteAdapter(url=f"sqlite:///{db_path.as_posix()}")
    adapter.create_many(
        "Orders",
        [
            {"order_id": 1, "customer": "A", "paid": True, "amount": 10},
            {"order_id": 2, "customer": "B", "paid": True, "amount": 20},
            {"order_id": 3, "customer": "C", "paid": False, "amount": 5},
        ],
    )
    rows = adapter.aggregate(
        "Orders",
        group_by="paid",
        metrics={"orders": "count(*)", "sum_amount": "sum(amount)"},
        having={"orders": 2},
        order_by="paid DESC",
        limit=10,
    )
    assert len(rows) == 1
    assert rows[0]["orders"] == 2
    assert rows[0]["sum_amount"] == 30


def test_sqlite_adapter_aggregate_rejects_invalid_metric() -> None:
    adapter = SQLiteAdapter(url="sqlite:///:memory:")
    with pytest.raises(QueryError, match="Invalid aggregate metric format"):
        adapter.aggregate("Orders", metrics={"total": "count(*); DROP TABLE Orders"})
