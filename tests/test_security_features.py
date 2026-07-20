from __future__ import annotations

import bcrypt
import pytest

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError


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

    def find(self, where, session=None):
        matched = []
        for doc in self.docs:
            ok = True
            for key, value in where.items():
                if doc.get(key) != value:
                    ok = False
                    break
            if ok:
                matched.append(dict(doc))
        return _FakeCursor(matched)

    def insert_one(self, doc, session=None):
        self.docs.append(dict(doc))
        return type("_R", (), {"inserted_id": len(self.docs)})

    def insert_many(self, docs, ordered=True, session=None):
        for doc in docs:
            self.docs.append(dict(doc))
        return type("_R", (), {"inserted_ids": list(range(1, len(docs) + 1))})

    def delete_many(self, where, session=None):
        deleted = 0
        keep = []
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in where.items()):
                deleted += 1
            else:
                keep.append(doc)
        self.docs = keep
        return type("_R", (), {"deleted_count": deleted})

    def update_many(self, where, values, session=None):
        matched = 0
        for doc in self.docs:
            if all(doc.get(k) == v for k, v in where.items()):
                matched += 1
                doc.update(values.get("$set", {}))
        return type("_R", (), {"matched_count": matched, "modified_count": matched})

    def count_documents(self, where, session=None):
        return len(list(self.find(where, session=session)))

    def create_index(self, keys, **options):
        return options.get("name", "idx")


class _FakeDB:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = _FakeCollection()
        return self.collections[name]

    def command(self, command):
        return {"ok": 1}


class _FakeClient:
    def __init__(self):
        self.db = _FakeDB()

    def __getitem__(self, name):
        return self.db

    def list_databases(self):
        return [{"name": "udom"}]

    def start_session(self):
        raise RuntimeError("sessions not needed for this test")


def _install_fake_mongo(db: UDOM) -> _FakeClient:
    fake_client = _FakeClient()

    def _fake_ensure():
        db.adapter._client = fake_client
        db.adapter._db = fake_client.db

    db.adapter._ensure_mongo = _fake_ensure
    return fake_client


def test_sql_hashes_sensitive_fields_on_create_and_update(tmp_path) -> None:
    db_file = tmp_path / "security_hashing.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")

    db.create("users", {"id": 1, "username": "username", "password": "password"})
    row = db.find("users", where={"id": 1})[0]
    assert row["password"] != "password"
    assert bcrypt.checkpw(b"password", row["password"].encode("utf-8"))

    db.update("users", {"password": "new-secret"}, where={"id": 1})
    updated = db.find("users", where={"id": 1})[0]
    assert bcrypt.checkpw(b"new-secret", updated["password"].encode("utf-8"))


def test_nosql_hashes_sensitive_fields_on_create() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    fake_client = _install_fake_mongo(db)

    db.create("users", {"username": "username", "password": "password"})
    stored = fake_client.db.collections["users"].docs[0]
    assert stored["password"] != "password"
    assert bcrypt.checkpw(b"password", stored["password"].encode("utf-8"))


def test_udom_verify_secret_for_sql_stored_hash(tmp_path) -> None:
    db_file = tmp_path / "verify_secret.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    db.create("users", {"id": 1, "username": "username", "password": "password"})

    stored_hash = db.find("users", where={"id": 1})[0]["password"]
    assert db.verify_secret("password", stored_hash) is True
    assert db.verify_secret("wrong-password", stored_hash) is False
    assert db.verify_secret("password", "not-a-bcrypt-hash") is False


def test_sql_security_audit_log_is_persisted_for_blocked_injection(tmp_path) -> None:
    db_file = tmp_path / "security_audit.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    db.create("users", {"id": 1, "username": "admin", "password": "secret"})

    with pytest.raises(QueryError):
        db.find("users", where={"id": "1 OR 1=1"})

    logs = db.find("security_logs")
    assert len(logs) == 1
    assert logs[0]["operation"] == "find"
    assert "invalid integer value" in logs[0]["reason"].lower()


def test_nosql_security_audit_redacts_sensitive_fields() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
    fake_client = _install_fake_mongo(db)

    with pytest.raises(QueryError):
        db.find("users", where={"username": "admin", "password": {"$gt": ""}})

    logs = fake_client.db.collections["security_logs"].docs
    assert len(logs) == 1
    assert logs[0]["operation"] == "find"
    assert "***REDACTED***" in logs[0]["input_snapshot"]


def test_rate_limiting_blocks_excess_queries(tmp_path) -> None:
    db_file = tmp_path / "rate_limit.db"
    db = UDOM(
        db_type="sql",
        db_instance="sqlite",
        url=f"sqlite:///{db_file.as_posix()}",
        rate_limit_enabled=True,
        rate_limit_max_requests=2,
        rate_limit_window_seconds=60,
    )
    db.create("users", {"id": 1, "username": "username"})

    assert len(db.find("users")) == 1
    assert len(db.find("users")) == 1
    with pytest.raises(QueryError, match="Rate limit exceeded"):
        db.find("users")

    logs = db.find("security_logs")
    assert any(entry["reason"] == "Rate limit exceeded" for entry in logs)


def test_umodel_verify_secret_field(tmp_path) -> None:
    from DBDuck.udom.models.umodel import UModel

    class User(UModel):
        id: int
        username: str
        password: str

    db_file = tmp_path / "model_verify_secret.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    User.bind(db)

    User(id=1, username="username", password="password").save()
    user = User.find_one(where={"id": 1})
    assert user is not None
    assert user.verify_secret("password", "password") is True
    assert user.verify_secret("password", "wrong-password") is False


def test_umodel_custom_sensitive_fields_are_hashed(tmp_path) -> None:
    from DBDuck.udom.models.umodel import UModel

    class Member(UModel):
        __sensitive_fields__ = ["pin"]
        id: int
        username: str
        pin: str

    db_file = tmp_path / "model_custom_sensitive.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    Member.bind(db)

    Member(id=1, username="username", pin="1234").save()
    row = db.find("Member", where={"id": 1})[0]
    assert row["pin"] != "1234"
    assert db.verify_secret("1234", row["pin"]) is True


def test_umodel_declared_sensitive_fields_replace_default_hash_list(tmp_path) -> None:
    from DBDuck.udom.models.umodel import UModel

    class Profile(UModel):
        __sensitive_fields__ = ["access_code"]
        id: int
        password: str
        access_code: str

    db_file = tmp_path / "model_sensitive_override.db"
    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_file.as_posix()}")
    Profile.bind(db)

    Profile(id=1, password="password", access_code="A-1").save()
    row = db.find("Profile", where={"id": 1})[0]
    assert row["password"] == "password"
    assert row["access_code"] != "A-1"
    assert db.verify_secret("A-1", row["access_code"]) is True
