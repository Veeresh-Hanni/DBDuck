from __future__ import annotations

from DBDuck.adapters.mysql_adapter import MySQLAdapter
from DBDuck.adapters.sqlite_adapter import SQLiteAdapter


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
