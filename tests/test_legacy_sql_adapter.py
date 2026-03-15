from __future__ import annotations

from DBDuck.udom.adapters.sql_adapter import ParameterizedSQL, SQLAdapter


def test_legacy_sql_adapter_create_uql_returns_parameterized_sql() -> None:
    adapter = SQLAdapter(url="sqlite:///:memory:")
    query = adapter.convert_uql("CREATE Users {name: 'Veeresh', age: 21, active: true}")

    assert isinstance(query, ParameterizedSQL)
    assert str(query) == 'INSERT INTO "Users" ("name", "age", "active") VALUES (:v_0, :v_1, :v_2);'
    assert query.params == {"v_0": "Veeresh", "v_1": 21, "v_2": True}


def test_legacy_sql_adapter_find_uql_parameterizes_where_literals() -> None:
    adapter = SQLAdapter(url="sqlite:///:memory:")
    query = adapter.convert_uql("FIND Users WHERE age >= 21 AND active = true ORDER BY name DESC LIMIT 5")

    assert isinstance(query, ParameterizedSQL)
    assert str(query) == 'SELECT * FROM "Users" WHERE "age" >= :w_0 AND "active" = :w_1 ORDER BY "name" DESC LIMIT 5;'
    assert query.params == {"w_0": 21, "w_1": 1}


def test_legacy_sql_adapter_delete_uql_parameterizes_where_literals() -> None:
    adapter = SQLAdapter(url="sqlite:///:memory:")
    query = adapter.convert_uql("DELETE Users WHERE name = 'Veeresh'")

    assert isinstance(query, ParameterizedSQL)
    assert str(query) == 'DELETE FROM "Users" WHERE "name" = :w_0;'
    assert query.params == {"w_0": "Veeresh"}
