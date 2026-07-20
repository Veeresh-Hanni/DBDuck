from __future__ import annotations

import pytest

from DBDuck import UDOM
from DBDuck.core.exceptions import ConnectionError, QueryError, TransactionError
from DBDuck.udom.models.umodel import UModel


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, order_by):
        field, direction = order_by[0]
        reverse = direction == -1
        self._docs.sort(key=lambda d: d.get(field), reverse=reverse)
        return self

    def limit(self, size):
        self._docs = self._docs[:size]
        return self

    def skip(self, size):
        self._docs = self._docs[size:]
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeCollection:
    def __init__(self):
        self.docs = []
        self.created_indexes = []

    def find(self, where, session=None):
        def _match(doc, clause):
            if not clause:
                return True
            if "$and" in clause:
                return all(_match(doc, c) for c in clause["$and"])
            if "$or" in clause:
                return any(_match(doc, c) for c in clause["$or"])
            for key, expected in clause.items():
                value = doc.get(key)
                if isinstance(expected, dict):
                    for op, target in expected.items():
                        if op == "$gt" and not (value > target):
                            return False
                        if op == "$lt" and not (value < target):
                            return False
                        if op == "$gte" and not (value >= target):
                            return False
                        if op == "$lte" and not (value <= target):
                            return False
                        if op == "$ne" and not (value != target):
                            return False
                        if op == "$exists" and bool(key in doc) != bool(target):
                            return False
                else:
                    if value != expected:
                        return False
            return True

        return _FakeCursor([d for d in self.docs if _match(d, where)])

    def insert_one(self, doc, session=None):
        self.docs.append(dict(doc))
        return type("_R", (), {"inserted_id": len(self.docs)})

    def insert_many(self, docs, ordered=True, session=None):
        for d in docs:
            self.docs.append(dict(d))
        return type("_R", (), {"inserted_ids": list(range(1, len(docs) + 1))})

    def delete_many(self, where, session=None):
        before = len(self.docs)
        keep = []
        for d in self.docs:
            if where and all(d.get(k) == v for k, v in where.items()):
                continue
            keep.append(d)
        self.docs = keep
        return type("_R", (), {"deleted_count": before - len(self.docs)})

    def update_many(self, where, values, session=None):
        matched = 0
        modified = 0
        for d in self.docs:
            if all(d.get(k) == v for k, v in where.items()):
                matched += 1
                for key, value in values.get("$set", {}).items():
                    d[key] = value
                modified += 1
        return type("_R", (), {"matched_count": matched, "modified_count": modified})

    def create_index(self, keys, **options):
        name = options.get("name") or "_".join([f"{k}_{v}" for k, v in keys])
        self.created_indexes.append({"name": name, "keys": keys, "options": options})
        return name

    def count_documents(self, where, session=None):
        if not where:
            return len(self.docs)
        count = 0
        for d in self.docs:
            ok = True
            for key, val in where.items():
                if isinstance(val, dict):
                    if "$gt" in val and not (d.get(key) > val["$gt"]):
                        ok = False
                    if "$lt" in val and not (d.get(key) < val["$lt"]):
                        ok = False
                    if "$gte" in val and not (d.get(key) >= val["$gte"]):
                        ok = False
                    if "$lte" in val and not (d.get(key) <= val["$lte"]):
                        ok = False
                    if "$ne" in val and not (d.get(key) != val["$ne"]):
                        ok = False
                else:
                    if d.get(key) != val:
                        ok = False
            if ok:
                count += 1
        return count


class _FailingCollection(_FakeCollection):
    def find(self, where, session=None):
        raise RuntimeError("driver find failure")


class _TransientFailingCollection(_FakeCollection):
    def __init__(self):
        super().__init__()
        self.failures_left = 1

    def insert_one(self, doc, session=None):
        if self.failures_left > 0:
            self.failures_left -= 1
            raise RuntimeError("temporary connection reset")
        return super().insert_one(doc, session=session)


class _FakeDB:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = _FakeCollection()
        return self.collections[name]

    def command(self, cmd):
        if cmd == "ping":
            return {"ok": 1}
        return {"ok": 0}


class _FakeSession:
    def __init__(self):
        self.started = False
        self.committed = False
        self.aborted = False
        self.ended = False

    def start_transaction(self):
        self.started = True

    def commit_transaction(self):
        self.committed = True

    def abort_transaction(self):
        self.aborted = True

    def end_session(self):
        self.ended = True


class _FakeClient:
    def __init__(self):
        self.db = _FakeDB()
        self.sessions = []

    def __getitem__(self, name):
        return self.db

    def list_databases(self):
        return [{"name": "udom"}]

    def start_session(self):
        s = _FakeSession()
        self.sessions.append(s)
        return s


def _install_fake_mongo(db: UDOM):
    fake_client = _FakeClient()

    def _fake_ensure():
        db.adapter._client = fake_client
        db.adapter._db = fake_client.db

    db.adapter._ensure_mongo = _fake_ensure
    return fake_client


def test_mongodb_find_order_by_limit_and_string_where() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    _install_fake_mongo(db)

    db.create_many(
        "events",
        [
            {"order_id": 1, "ok": True},
            {"order_id": 3, "ok": True},
            {"order_id": 2, "ok": False},
        ],
    )
    rows = db.find("events", where="ok = true", order_by="-order_id", limit=1)
    assert len(rows) == 1
    assert rows[0]["order_id"] == 3


def test_mongodb_transaction_commit_and_rollback() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    client = _install_fake_mongo(db)

    db.begin()
    db.create("events", {"ok": True})
    db.commit()
    assert client.sessions[-1].committed is True
    assert client.sessions[-1].ended is True

    try:
        with db.transaction():
            db.create("events", {"ok": False})
            raise RuntimeError("force rollback")
    except RuntimeError:
        pass
    assert client.sessions[-1].aborted is True
    assert client.sessions[-1].ended is True


def test_mongodb_connection_error_is_mapped(monkeypatch) -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    monkeypatch.setattr(db.adapter._conn_manager, "get_client", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    with pytest.raises(ConnectionError):
        db.execute("ping")


def test_mongodb_query_error_is_mapped() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    fake_client = _FakeClient()
    fake_client.db.collections["events"] = _FailingCollection()

    def _fake_ensure():
        db.adapter._client = fake_client
        db.adapter._db = fake_client.db

    db.adapter._ensure_mongo = _fake_ensure
    with pytest.raises(QueryError):
        db.find("events", where={"ok": True})


def test_mongodb_rejects_operator_injection_in_where_mapping() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    _install_fake_mongo(db)
    db.create("events", {"username": "admin", "password": "secret"})

    with pytest.raises(QueryError, match="Mongo operator expressions are not allowed"):
        db.find("events", where={"username": "admin", "password": {"$gt": ""}})


def test_mongodb_rejects_top_level_dollar_where_mapping() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    _install_fake_mongo(db)

    with pytest.raises(QueryError, match="valid identifier"):
        db.find("events", where={"$where": "function() { return true; }"})


def test_mongodb_query_time_connection_error_is_mapped() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    fake_client = _FakeClient()

    class _ConnFailingCollection(_FakeCollection):
        def find(self, where, session=None):
            raise RuntimeError("connection refused")

    fake_client.db.collections["events"] = _ConnFailingCollection()

    def _fake_ensure():
        db.adapter._client = fake_client
        db.adapter._db = fake_client.db

    db.adapter._ensure_mongo = _fake_ensure
    with pytest.raises(ConnectionError):
        db.find("events", where={"ok": True})


def test_mongodb_transaction_error_is_mapped() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    _install_fake_mongo(db)
    with pytest.raises(TransactionError):
        db.commit()


def test_mongodb_retry_transient_error(monkeypatch) -> None:
    db = UDOM(
        db_type="nosql",
        db_instance="mongodb",
        url="mongodb://localhost:27017/udom",
        retry_attempts=3,
        retry_backoff_ms=1,
    )
    fake_client = _FakeClient()
    fake_client.db.collections["events"] = _TransientFailingCollection()

    def _fake_ensure():
        db.adapter._client = fake_client
        db.adapter._db = fake_client.db

    db.adapter._ensure_mongo = _fake_ensure
    monkeypatch.setattr("DBDuck.udom.adapters.nosql_adapter.time.sleep", lambda *_: None)
    result = db.create("events", {"ok": True})
    assert "inserted_id" in result


def test_mongodb_ensure_indexes() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    fake_client = _install_fake_mongo(db)
    outcome = db.ensure_indexes(
        "events",
        [
            {
                "fields": [{"name": "type", "order": "asc"}, {"name": "ts", "order": "desc"}],
                "options": {"name": "idx_type_ts"},
            }
        ],
    )
    assert outcome["count"] == 1
    collection = fake_client.db.collections["events"]
    assert collection.created_indexes[0]["name"] == "idx_type_ts"


def test_mongodb_model_ensure_indexes() -> None:
    class EventModel(UModel):
        __collection__ = "events"
        __indexes__ = [
            {"fields": [{"name": "type", "order": "asc"}], "options": {"name": "idx_type"}}
        ]
        type: str
        ok: bool

    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    fake_client = _install_fake_mongo(db)
    EventModel.bind(db)
    outcome = EventModel.ensure_indexes()
    assert outcome["count"] == 1
    collection = fake_client.db.collections["events"]
    assert collection.created_indexes[0]["name"] == "idx_type"


def test_mongodb_aggregate_pipeline_builder() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    captured: dict[str, object] = {}

    def _capture(query, params=None):
        captured["query"] = query
        return [{"type": "login", "total": 2}]

    db.adapter.run_native = _capture  # type: ignore[method-assign]
    rows = db.aggregate(
        "events",
        group_by="type",
        metrics={"total": "count(*)"},
        where={"ok": True},
        having={"total": 2},
        order_by="total DESC",
        limit=5,
    )
    assert rows[0]["total"] == 2
    query = captured["query"]
    assert query["aggregate"] == "events"
    pipeline = query["pipeline"]
    assert pipeline[0] == {"$match": {"ok": True}}
    assert pipeline[1]["$group"]["total"] == {"$sum": 1}
    assert pipeline[-1] == {"$limit": 5}
