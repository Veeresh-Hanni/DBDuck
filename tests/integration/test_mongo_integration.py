from __future__ import annotations

import os

import pytest

from DBDuck import UDOM
from DBDuck.core.exceptions import ConnectionError

from ._helpers import unique_entity


RUN = os.getenv("RUN_MONGO_INTEGRATION") == "1"
RUN_TX = os.getenv("RUN_MONGO_TX_INTEGRATION") == "1"
URL = os.getenv("MONGO_TEST_URL", "mongodb://localhost:27017/udom_test")

pytestmark = pytest.mark.skipif(not RUN, reason="Set RUN_MONGO_INTEGRATION=1 to run Mongo integration tests")


def test_mongo_ping_and_crud_roundtrip() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url=URL, db_name="udom_test")
    entity = unique_entity("integration_events")
    assert db.ping().get("ok") == 1

    db.create(entity, {"type": "health", "ok": True})
    rows = db.find(entity, where={"type": "health"}, limit=5)
    assert isinstance(rows, list)
    assert len(rows) >= 1

    db.delete(entity, {"type": "health"})


def test_mongo_native_pagination_roundtrip() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url=URL, db_name="udom_test")
    entity = unique_entity("integration_page")

    db.create_many(entity, [{"seq": idx, "ok": True} for idx in range(1, 6)])
    page = db.find_page(entity, page=2, page_size=2, where={"ok": True}, order_by="seq ASC")
    seqs = [row["seq"] for row in page["items"]]
    assert seqs == [3, 4]
    assert page["total"] == 5
    assert page["total_pages"] == 3

    db.delete(entity, {"ok": True})


def test_mongo_connection_failure_maps_to_connection_error() -> None:
    bad_url = "mongodb://127.0.0.1:1/udom_test"
    db = UDOM(
        db_type="nosql",
        db_instance="mongodb",
        url=bad_url,
        db_name="udom_test",
        connect_timeout_ms=250,
    )

    with pytest.raises(ConnectionError):
        db.ping()


@pytest.mark.skipif(not RUN_TX, reason="Set RUN_MONGO_TX_INTEGRATION=1 for Mongo transaction integration")
def test_mongo_transaction_rollback() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url=URL, db_name="udom_test")
    entity = unique_entity("integration_tx")

    with pytest.raises(RuntimeError):
        with db.transaction():
            db.create(entity, {"type": "rollback", "ok": False})
            raise RuntimeError("force rollback")

    rows = db.find(entity, where={"type": "rollback"}, limit=5)
    assert rows == []
