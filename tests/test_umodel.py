from __future__ import annotations

from datetime import date, datetime, time

import pytest

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError
from DBDuck.models import UModel


class Order(UModel):
    __entity__ = "Orders"
    order_id: int
    customer: str
    paid: bool


class Event(UModel):
    __collection__ = "events"
    type: str
    ok: bool


class StrictProfile(UModel):
    __entity__ = "profiles"
    age: int
    score: float
    active: bool
    tags: list[str]
    nickname: str | None


class CalendarEvent(UModel):
    __entity__ = "calendar_events"
    title: str
    starts_at: datetime
    event_date: date
    reminder_at: time | None


def test_umodel_sql_save_find_and_find_one(tmp_path) -> None:
    db_file = tmp_path / "umodel_sql.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    Order.bind(db)

    Order(order_id=1, customer="A", paid=True).save()
    rows = Order.find(where={"paid": True})
    assert len(rows) == 1
    assert isinstance(rows[0], Order)
    one = Order.find_one(where={"order_id": 1})
    assert one is not None
    assert one.customer == "A"


def test_umodel_sql_bulk_create(tmp_path) -> None:
    db_file = tmp_path / "umodel_bulk.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    Order.bind(db)
    result = Order.bulk_create(
        [
            Order(order_id=101, customer="A", paid=True),
            {"order_id": 102, "customer": "B", "paid": False},
        ]
    )
    assert result["rows_affected"] == 2


def test_umodel_nosql_mongo_style_with_non_mongo_backend() -> None:
    db = UDOM(db_type="nosql", db_instance="redis", url="redis://localhost:6379")
    Event.bind(db)
    created = Event(type="login", ok=True).save()
    assert created["insert"] == "events"
    found = Event.find(where={"ok": True})
    assert isinstance(found, list)


def test_umodel_delete_infers_id(tmp_path) -> None:
    db_file = tmp_path / "umodel_delete.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    Order.bind(db)
    Order(order_id=201, customer="Z", paid=False).save()
    # Query inserted row, then delete by inferred id.
    row = db.find("Orders", where={"order_id": 201})[0]
    obj = Order.from_dict(row)
    result = obj.delete()
    assert result["rows_affected"] == 1


def test_umodel_strict_type_coercion_success(tmp_path) -> None:
    db_file = tmp_path / "umodel_strict_ok.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    StrictProfile.bind(db)
    p = StrictProfile(age="21", score="3.5", active="true", tags=["new", "vip"], nickname=None)
    p.save()
    rows = StrictProfile.find(where={"age": 21})
    assert len(rows) == 1
    assert isinstance(rows[0].age, int)
    assert isinstance(rows[0].score, float)
    assert rows[0].active is True


def test_umodel_strict_type_validation_error() -> None:
    p = StrictProfile(age="abc", score=2.0, active=True, tags=["x"], nickname=None)
    with pytest.raises(QueryError):
        p.validate()


def test_umodel_bulk_create_mapping_validation(tmp_path) -> None:
    db_file = tmp_path / "umodel_bulk_validation.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    StrictProfile.bind(db)
    with pytest.raises(QueryError):
        StrictProfile.bulk_create(
            [
                {"age": "23", "score": "1.5", "active": "yes", "tags": ["a"], "nickname": None},
                {"age": "bad", "score": "1.2", "active": True, "tags": ["b"], "nickname": None},
            ]
        )


def test_umodel_date_time_roundtrip_sql(tmp_path) -> None:
    db_file = tmp_path / "umodel_datetime.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    CalendarEvent.bind(db)

    event = CalendarEvent(
        title="Launch",
        starts_at="2026-03-15T09:30:00",
        event_date="2026-03-16",
        reminder_at="08:45:00",
    )
    event.save()

    one = CalendarEvent.find_one(where={"title": "Launch"})
    assert one is not None
    assert isinstance(one.starts_at, datetime)
    assert isinstance(one.event_date, date)
    assert isinstance(one.reminder_at, time)
    assert one.starts_at == datetime(2026, 3, 15, 9, 30, 0)
    assert one.event_date == date(2026, 3, 16)
    assert one.reminder_at == time(8, 45, 0)


def test_umodel_date_time_validation_error() -> None:
    bad = CalendarEvent(
        title="Broken",
        starts_at="not-a-datetime",
        event_date="2026-03-16",
        reminder_at=None,
    )
    with pytest.raises(QueryError, match="starts_at"):
        bad.validate()
