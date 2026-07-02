"""
Regression tests for UModel:

1. Sensitive-field round trip (save -> hashed in DB -> stripped from to_dict()
   by default -> present via verify_secret / include_sensitive=True).
   Guards against the "to_dict() strips password before it's hashed" bug.

2. .select() projection on UModel.query().find() must not crash on
   missing non-selected required fields.
   Guards against the "validate(only_fields=...) wired but disabled" bug.

Run with:
    pip install pytest --break-system-packages
    pytest test_umodel_sensitive_and_projection.py -v

Uses an in-memory sqlite DB so tests are fast and fully isolated.
Adjust the import path for `UDOM` / `UModel` / `Column` / field types
below to match your actual package layout if it differs.
"""

from __future__ import annotations

import pytest

from DBDuck import UDOM
from DBDuck.models import (
    UModel,
    Column,
    CharField,
    IntegerField,
    BooleanField,
    AutoField,
)


# ---------------------------------------------------------------------------
# Test model
# ---------------------------------------------------------------------------

class User(UModel):
    __entity__ = "users"

    # AutoField (not plain IntegerField) so validate() treats id as
    # optional/auto-populated on save() -- matches your working scripts.
    id = Column(AutoField, primary_key=True)
    name = Column(CharField, nullable=False)
    email = Column(CharField, unique=True, nullable=False)
    password = Column(CharField, nullable=False)
    role = Column(CharField, default="user")
    active = Column(BooleanField, default=True)
    age = Column(IntegerField, nullable=True)

    __sensitive_fields__ = ["password"]


@pytest.fixture()
def db():
    """Fresh in-memory sqlite DB per test."""
    instance = UDOM(url="sqlite:///:memory:")
    User.bind(instance)
    yield instance
    instance.close()


# ---------------------------------------------------------------------------
# 1. Sensitive-field round trip
# ---------------------------------------------------------------------------

class TestSensitiveFieldRoundTrip:
    def test_save_hashes_password_in_db(self, db):
        """Root-cause guard: to_dict() defaulting to include_sensitive=False
        must NOT cause save() to drop the password before hashing."""
        u = User(id=1, name="Ganesh", email="ganesh@example.com", password="plaintext123", age=28)
        u.save()

        stored = User.query().where(email="ganesh@example.com").first()
        assert stored is not None, "row was not created"
        raw_password = stored.to_dict(include_sensitive=True).get("password")

        assert raw_password is not None, (
            "password field is missing/NULL in DB — likely stripped by "
            "to_dict() before hashing in save()"
        )
        assert raw_password != "plaintext123", "password was stored in plaintext, not hashed"
        assert raw_password.startswith("$2b$") or raw_password.startswith("$2a$"), (
            f"expected a bcrypt hash, got: {raw_password!r}"
        )

    def test_bulk_create_hashes_password_in_db(self, db):
        """Same root-cause guard, via the bulk_create() path."""
        rows = [
            User(id=1, name="Suresh", email="suresh@example.com", password="hunter2", role="editor", age=32),
            User(id=2, name="Mahesh", email="mahesh@example.com", password="hunter3", age=22),
        ]
        User.bulk_create(rows)

        stored = User.query().where(email="suresh@example.com").first()
        assert stored is not None
        raw_password = stored.to_dict(include_sensitive=True).get("password")
        assert raw_password not in (None, "hunter2"), "bulk_create did not hash the password"
        assert raw_password.startswith("$2b$") or raw_password.startswith("$2a$")

    def test_update_hashes_new_password_in_db(self, db):
        """Guard the update() path specifically -- a common place for the
        same to_dict()-strips-the-field bug to hide."""
        u = User(id=1, name="Ganesh", email="ganesh2@example.com", password="oldpass", age=28)
        u.save()

        User.query().where(email="ganesh2@example.com").update({"password": "newpass"})

        stored = User.query().where(email="ganesh2@example.com").first()
        raw_password = stored.to_dict(include_sensitive=True).get("password")
        assert raw_password is not None
        assert raw_password != "newpass", "password was updated in plaintext, not hashed"
        assert stored.verify_secret("password", "newpass") is True

    def test_to_dict_excludes_password_by_default(self, db):
        """Read-side guard: default to_dict() must NOT leak the hash."""
        u = User(id=1, name="Ganesh", email="ganesh3@example.com", password="plaintext123", age=28)
        u.save()

        stored = User.query().where(email="ganesh3@example.com").first()
        safe_dict = stored.to_dict()
        assert "password" not in safe_dict, "password leaked through default to_dict()"

    def test_to_dict_includes_password_when_explicitly_requested(self, db):
        u = User(id=1, name="Ganesh", email="ganesh4@example.com", password="plaintext123", age=28)
        u.save()

        stored = User.query().where(email="ganesh4@example.com").first()
        full_dict = stored.to_dict(include_sensitive=True)
        assert "password" in full_dict

    def test_verify_secret_correct_and_incorrect(self, db):
        u = User(id=1, name="Ganesh", email="ganesh5@example.com", password="correcthorse", age=28)
        u.save()

        stored = User.query().where(email="ganesh5@example.com").first()
        assert stored.verify_secret("password", "correcthorse") is True
        assert stored.verify_secret("password", "wrongpassword") is False
        assert stored.verify_secret("password", "") is False


# ---------------------------------------------------------------------------
# 2. select() projection should not crash required-field validation
# ---------------------------------------------------------------------------

class TestSelectProjection:
    def test_select_subset_of_fields_does_not_raise(self, db):
        """Root-cause guard: querying a subset of columns via .select()
        must not trip 'Missing required model field' validation on the
        columns that were intentionally not selected."""
        User(id=1, name="Ganesh", email="ganesh6@example.com", password="x", age=28).save()

        try:
            users = User.query().select("id", "name", "age").find()
        except Exception as exc:  # noqa: BLE001 - we want to see any exception type here
            pytest.fail(f"select() projection raised unexpectedly: {exc!r}")

        assert len(users) >= 1
        assert users[0].name == "Ganesh"

    def test_select_subset_only_populates_selected_fields(self, db):
        User(id=1, name="Suresh", email="suresh6@example.com", password="x", role="editor", age=32).save()

        users = User.query().where(name="Suresh").select("id", "name", "age").find()
        assert len(users) == 1
        row = users[0].to_dict(include_sensitive=True)

        # selected fields present
        assert "id" in row
        assert "name" in row
        assert "age" in row
        assert set(row) == {"id", "name", "age"}

    def test_full_query_without_select_still_validates_required_fields(self, db):
        """Make sure the only_fields relaxation doesn't accidentally
        disable validation for normal, non-projected queries."""
        User(id=1, name="Mahesh", email="mahesh6@example.com", password="x", age=22).save()

        users = User.query().where(name="Mahesh").find()
        assert len(users) == 1
        full = users[0].to_dict(include_sensitive=True)
        assert full["email"] == "mahesh6@example.com"
        assert full["name"] == "Mahesh"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
