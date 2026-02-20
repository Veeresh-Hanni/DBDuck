from __future__ import annotations

import pytest

from DBDuck import UDOM
from DBDuck.core.base_adapter import BaseAdapter
from DBDuck.core.exceptions import QueryError


def test_sql_adapter_auto_selected_from_url(tmp_path) -> None:
    db_file = tmp_path / "auto_route.db"
    db = UDOM(db_type="sql", url=f"sqlite:///{db_file.as_posix()}")
    assert db.db_instance == "sqlite"
    result = db.create("Orders", {"order_id": 11, "customer": "A", "paid": True})
    assert result["rows_affected"] == 1


def test_where_string_injection_is_rejected(tmp_path) -> None:
    db_file = tmp_path / "inject_guard.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    db.create("Orders", {"order_id": 22, "customer": "B", "paid": True})
    with pytest.raises(QueryError):
        db.find("Orders", where="order_id = 22; DROP TABLE Orders")


def test_schema_validation_rejects_invalid_entity(tmp_path) -> None:
    db_file = tmp_path / "schema_guard.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    with pytest.raises(QueryError):
        db.create("Invalid Name", {"x": 1})


def test_base_adapter_abstract_contract_enforced() -> None:
    class _IncompleteAdapter(BaseAdapter):
        def run_native(self, query, params=None):
            return query

        def convert_uql(self, uql_query):
            return uql_query

    with pytest.raises(TypeError):
        _IncompleteAdapter()
