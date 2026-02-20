from __future__ import annotations

from DBDuck import UDOM


def test_transaction_commit(tmp_path) -> None:
    db_file = tmp_path / "tx_commit.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    db.begin()
    db.create("Orders", {"order_id": 1, "customer": "A", "paid": True})
    db.commit()
    rows = db.find("Orders", where={"order_id": 1})
    assert len(rows) == 1


def test_transaction_rollback(tmp_path) -> None:
    db_file = tmp_path / "tx_rollback.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    db.begin()
    db.create("Orders", {"order_id": 2, "customer": "B", "paid": False})
    db.rollback()
    rows = db.find("Orders", where={"order_id": 2})
    assert rows == []


def test_transaction_context_manager(tmp_path) -> None:
    db_file = tmp_path / "tx_context.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    with db.transaction():
        db.create("Orders", {"order_id": 3, "customer": "C", "paid": True})
    rows = db.find("Orders", where={"order_id": 3})
    assert len(rows) == 1
