from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import pytest

from DBDuck import UDOM
from DBDuck.core.exceptions import ConnectionError

from ._helpers import env_value, require_env_flag, unique_entity


SQL_CASES = [
    pytest.param(
        "mysql",
        "RUN_MYSQL_INTEGRATION",
        lambda: env_value("MYSQL_TEST_URL", "mysql+pymysql://username:pass@localhost:3306/udom_test"),
        id="mysql",
        marks=require_env_flag("RUN_MYSQL_INTEGRATION", reason="Set RUN_MYSQL_INTEGRATION=1 to run MySQL integration"),
    ),
    pytest.param(
        "postgres",
        "RUN_POSTGRES_INTEGRATION",
        lambda: env_value("POSTGRES_TEST_URL", "postgresql+psycopg2://username:pass@localhost:5432/udom_test"),
        id="postgres",
        marks=require_env_flag(
            "RUN_POSTGRES_INTEGRATION", reason="Set RUN_POSTGRES_INTEGRATION=1 to run PostgreSQL integration"
        ),
    ),
    pytest.param(
        "mssql",
        "RUN_MSSQL_INTEGRATION",
        lambda: env_value(
            "MSSQL_TEST_URL",
            "mssql+pyodbc://username:pass@localhost:1433/udom_test?driver=ODBC+Driver+17+for+SQL+Server",
        ),
        id="mssql",
        marks=require_env_flag("RUN_MSSQL_INTEGRATION", reason="Set RUN_MSSQL_INTEGRATION=1 to run SQL Server integration"),
    ),
]


def _invalid_sql_url(url: str) -> str:
    parsed = urlsplit(url)
    host = parsed.hostname or "127.0.0.1"
    username = parsed.username or ""
    password = parsed.password or ""
    auth = username
    if password:
        auth += f":{password}"
    if auth:
        auth += "@"
    netloc = f"{auth}{host}:1"
    if parsed.port and parsed.port == 1:
        netloc = f"{auth}{host}:2"
    if parsed.query:
        query = urlencode(dict(parse_qsl(parsed.query, keep_blank_values=True)))
    else:
        query = parsed.query
    return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))


@pytest.mark.parametrize(("db_instance", "_flag", "url_factory"), SQL_CASES)
def test_sql_backend_ping_and_crud_roundtrip(db_instance: str, _flag: str, url_factory) -> None:
    url = url_factory()
    db = UDOM(db_type="sql", db_instance=db_instance, url=url)
    entity = unique_entity(f"it_{db_instance}")

    ping = db.ping()
    assert ping

    create_result = db.create(entity, {"id": 1, "name": "alpha", "paid": True})
    assert create_result["rows_affected"] == 1

    rows = db.find(entity, where={"id": 1}, limit=5)
    assert len(rows) == 1
    assert rows[0]["name"] == "alpha"

    update_result = db.update(entity, {"name": "beta"}, where={"id": 1})
    assert update_result["rows_affected"] == 1
    assert db.count(entity, where={"name": "beta"}) == 1

    delete_result = db.delete(entity, {"id": 1})
    assert delete_result["rows_affected"] == 1


@pytest.mark.parametrize(("db_instance", "_flag", "url_factory"), SQL_CASES)
def test_sql_backend_transaction_roundtrip(db_instance: str, _flag: str, url_factory) -> None:
    url = url_factory()
    db = UDOM(db_type="sql", db_instance=db_instance, url=url)
    entity = unique_entity(f"tx_{db_instance}")

    with db.transaction():
        db.create(entity, {"id": 1, "name": "inside_tx", "paid": False})

    assert db.count(entity, where={"id": 1}) == 1


@pytest.mark.parametrize(("db_instance", "_flag", "url_factory"), SQL_CASES)
def test_sql_backend_transaction_rollback(db_instance: str, _flag: str, url_factory) -> None:
    url = url_factory()
    db = UDOM(db_type="sql", db_instance=db_instance, url=url)
    entity = unique_entity(f"rb_{db_instance}")

    with pytest.raises(RuntimeError):
        with db.transaction():
            db.create(entity, {"id": 1, "name": "rollback_me", "paid": False})
            raise RuntimeError("force rollback")

    assert db.count(entity, where={"id": 1}) == 0


@pytest.mark.parametrize(("db_instance", "_flag", "url_factory"), SQL_CASES)
def test_sql_backend_native_pagination(db_instance: str, _flag: str, url_factory) -> None:
    url = url_factory()
    db = UDOM(db_type="sql", db_instance=db_instance, url=url)
    entity = unique_entity(f"pg_{db_instance}")

    db.create_many(
        entity,
        [{"id": idx, "name": f"item-{idx}", "paid": bool(idx % 2)} for idx in range(1, 6)],
    )
    page = db.find_page(entity, page=2, page_size=2, order_by="id ASC")
    ids = [row["id"] for row in page["items"]]
    assert ids == [3, 4]
    assert page["total"] == 5
    assert page["total_pages"] == 3


@pytest.mark.parametrize(("db_instance", "_flag", "url_factory"), SQL_CASES)
def test_sql_backend_connection_failure_maps_to_connection_error(db_instance: str, _flag: str, url_factory) -> None:
    bad_url = _invalid_sql_url(url_factory())
    db = UDOM(db_type="sql", db_instance=db_instance, url=bad_url)

    with pytest.raises(ConnectionError):
        db.ping()