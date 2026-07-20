from __future__ import annotations

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError


def test_nosql_injection_on_dbduck() -> None:
    db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/dbduck_test")
    entity = "users_collection"

    db.create(entity, {"username": "username", "password": "password"})
    db.create(entity, {"username": "admin", "password": "password"})

    print("\n--- Starting MongoDB Security Tests ---")

    malicious_query = {"username": "admin", "password": {"$gt": ""}}
    try:
        db.find(entity, where=malicious_query)
        print("❌ SECURITY VULNERABILITY: NoSQL Operator Injection successful!")
    except QueryError as exc:
        print(f"✅ PASS: NoSQL Operator Injection rejected: {exc}")

    js_payload = {"$where": "function() { return true; }"}
    try:
        db.find(entity, where=js_payload)
        print("❌ SECURITY VULNERABILITY: JavaScript Injection successful!")
    except QueryError as exc:
        print(f"✅ PASS: Malicious JavaScript rejected: {exc}")


if __name__ == "__main__":
    test_nosql_injection_on_dbduck()
