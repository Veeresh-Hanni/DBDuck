from __future__ import annotations

import pytest

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError


def test_sqlite_create_view_and_query_it(tmp_path) -> None:
    db_file = tmp_path / "sql_objects.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}", admin_mode=True)

    db.create_many(
        "Orders",
        [
            {"order_id": 1, "customer": "A", "paid": True},
            {"order_id": 2, "customer": "B", "paid": False},
        ],
    )
    db.create_view(
        "PaidOrders",
        "SELECT order_id, customer FROM Orders WHERE paid = 1",
        replace=True,
    )

    rows = db.find("PaidOrders")
    assert len(rows) == 1
    assert rows[0]["order_id"] == 1
    assert rows[0]["customer"] == "A"


def test_sql_object_drop_requires_admin_mode(tmp_path) -> None:
    db_file = tmp_path / "sql_drop_requires_admin.db"
    admin_db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}", admin_mode=True)
    user_db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")

    admin_db.create("Orders", {"order_id": 1, "paid": True})
    admin_db.create_view("PaidOrders", "SELECT order_id FROM Orders WHERE paid = 1")

    with pytest.raises(QueryError, match="drop_view requires explicit admin_mode=True"):
        user_db.drop_view("PaidOrders")

    admin_db.drop_view("PaidOrders")


def test_sqlite_call_function_returns_scalar(tmp_path) -> None:
    db_file = tmp_path / "sql_functions.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}", admin_mode=True)

    assert db.call_function("abs", [-5]) == 5


def test_sqlite_procedure_function_and_event_management_are_explicitly_unsupported(tmp_path) -> None:
    db_file = tmp_path / "sql_unsupported_objects.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}", admin_mode=True)

    with pytest.raises(QueryError, match="stored procedures are not supported for sqlite"):
        db.create_procedure("sync_orders", "() BEGIN SELECT 1; END", replace=True)
    with pytest.raises(QueryError, match="stored procedures are not supported for sqlite"):
        db.call_procedure("sync_orders")
    with pytest.raises(QueryError, match="function creation is not supported for sqlite"):
        db.create_function("calc_tax", "(amount INT) RETURNS INT RETURN amount", replace=True)
    with pytest.raises(QueryError, match="database events are currently supported only for mysql"):
        db.create_event("nightly_cleanup", "EVERY 1 DAY", "DELETE FROM Orders")
