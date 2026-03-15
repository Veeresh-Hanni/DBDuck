# DBDuck

<p align="center">
  <img src="docs/assets/dbduck-logo.png" alt="DBDuck Logo" width="320" bg="black" />
</p>

**Universal Data Object Model (UDOM) for SQL and NoSQL.**

DBDuck gives one API for data operations across engines.

## Current Stage

- Stable focus: `SQL` + `NoSQL (MongoDB)`
- Next phase: Graph, AI, Vector

## Supported Backends

Current production-capable backends in DBDuck:

- `SQLite`
- `MySQL`
- `PostgreSQL`
- `SQL Server`
- `MongoDB`

These are the current officially supported real backends for DBDuck core UDOM workflows.

Backend names that may appear in config but are not yet production-complete should be treated as planned or experimental.

## Install

```bash
pip install .
# for tests and tooling
pip install .[dev]
# for MongoDB support
pip install .[mongo]
# for SQL Server support
pip install .[mssql]
# install all optional backend extras
pip install .[all]
```

## Backend Hardening Config

Runtime behavior is environment-configurable with secure defaults.

- See `.env.example` for all settings.
- Sensitive deployment values should come from your secret manager.

Key options:

- `DBDUCK_SQL_POOL_SIZE`, `DBDUCK_SQL_MAX_OVERFLOW`
- `DBDUCK_MONGO_MAX_POOL_SIZE`, `DBDUCK_MONGO_CONNECT_TIMEOUT_MS`
- `DBDUCK_MONGO_RETRY_ATTEMPTS`, `DBDUCK_MONGO_RETRY_BACKOFF_MS`
- `DBDUCK_ALLOW_UNSAFE_WHERE_STRINGS=false` (recommended for production)
- `DBDUCK_HASH_SENSITIVE_FIELDS=true`
- `DBDUCK_BCRYPT_ROUNDS=12`
- `DBDUCK_SECURITY_AUDIT_ENABLED=true`
- `DBDUCK_SECURITY_AUDIT_ENTITY=security_logs`
- `DBDUCK_RATE_LIMIT_ENABLED=false`
- `DBDUCK_RATE_LIMIT_MAX_REQUESTS=60`
- `DBDUCK_RATE_LIMIT_WINDOW_SECONDS=60`

## Quick Start

```python
from DBDuck import UDOM

# SQL (MySQL / PostgreSQL / SQLite)
db = UDOM(db_type="sql", db_instance="mysql", url="mysql+pymysql://user:pass@localhost:3306/udom")
db.create("Orders", {"order_id": 101, "customer": "A", "paid": True})
print(db.find("Orders", where={"paid": True}, limit=10))

# Explicit transactions
db.begin()
db.create("Orders", {"order_id": 102, "customer": "B", "paid": False})
db.commit()

# Transaction context manager
with db.transaction():
    db.create("Orders", {"order_id": 103, "customer": "C", "paid": True})

# NoSQL (MongoDB)
nosql_db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
print(nosql_db.execute("ping"))
print(nosql_db.create("events", {"type": "login", "ok": True}))
print(nosql_db.find("events", where={"ok": True}))
print(
    db.aggregate(
        "Orders",
        group_by="paid",
        metrics={"total_orders": "count(*)"},
        order_by="paid DESC",
    )
)

# BCrypt secret verification
db.create("users", {"id": 1, "username": "veeresh", "password": "plain-secret"})
user = db.find("users", where={"id": 1})[0]
assert db.verify_secret("plain-secret", user["password"]) is True
```

Model-level sensitive fields:

```python
from DBDuck.udom.models.umodel import UModel

class Member(UModel):
    __sensitive_fields__ = ["pin"]
    id: int
    username: str
    pin: str
```

## Core API

- `create(entity, data)`
- `create_many(entity, rows)`
- `find(entity, where=None, order_by=None, limit=None)`
- `find_page(entity, page=1, page_size=20, where=None, order_by=None)`
- `delete(entity, where)`
- `update(entity, data, where)`
- `count(entity, where=None)`
- `aggregate(entity, group_by=None, metrics=None, where=None, having=None, order_by=None, limit=None, pipeline=None)`
- `execute(native_query)`
- `uquery(uql)`
- `uexecute(uql)`
- `begin()`
- `commit()`
- `rollback()`
- `transaction()`
- `ping()`
- `close()`
- `ensure_indexes(entity, indexes)` (NoSQL/Mongo)

## Production Architecture

```text
DBDuck/
  core/
    adapter_router.py
    base_adapter.py
    connection_manager.py
    exceptions.py
    mongo_connection_manager.py
    schema.py
    transaction.py
  adapters/
    mysql_adapter.py
    mssql_adapter.py
    postgres_adapter.py
    sqlite_adapter.py
  udom/
    udom.py
  utils/
    logger.py
```

Design highlights:
- Adapter pattern keeps backend-specific logic out of `UDOM`.
- SQL adapters use SQLAlchemy with parameterized execution and connection pooling.
- MongoDB NoSQL adapter supports pooled client management, safe filter parsing, and transactions.
- `ConnectionManager` provides lazy, thread-safe engine/session reuse.
- Structured logging captures query, error, and connection events.

## Recent Changes

- Enforced full `BaseAdapter` abstract contract for all adapters.
- Added adapter auto-router for SQL dialect selection from `db_instance` / URL.
- Added thread-safe SQL and Mongo connection managers with lifecycle cleanup.
- Added transaction safety for SQL + Mongo (`begin`, `commit`, `rollback`, `transaction`).
- Added centralized schema validation for `create/find/delete`.
- Added stronger injection defenses:
  - SQL string `where` parsing + parameter binding.
  - Mongo filter parsing with unsafe token rejection.
- Added automatic BCrypt hashing for sensitive fields like `password`, `secret`, and token fields.
- Added security audit trail persistence to `security_logs` for blocked injection attempts and rate-limit events.
- Added in-memory rate limiting controls for UDOM operations.
- Added custom exception mapping across SQL + Mongo:
  - `DatabaseError`, `ConnectionError`, `QueryError`, `TransactionError`.
- Added structured logging for connection/query/transaction events and errors.
- Added masked execution errors for SQL and Mongo so raw driver/database details are not exposed to callers.
- Added batch operations (`create_many`) for SQL + Mongo.
- Added health/lifecycle methods: `ping()` and `close()`.
- Added `verify_secret(...)` for BCrypt password/secret verification.
- Added `UModel.__sensitive_fields__` for model-level sensitive field hashing.
- Added real backend integration test scaffolding for `MySQL`, `PostgreSQL`, `SQL Server`, and `MongoDB`.
- Added native backend pagination support for SQL and Mongo-backed `find_page()`.
- Added test coverage for routing, transactions, validation, error handling, hashing, audit logs, rate limiting, and integration scaffolding.

## CI/CD (Tests)

GitHub Actions workflow is included at:

- `.github/workflows/ci.yml`

It runs on push and pull requests:

- Python `3.10`, `3.11`, `3.12`
- `pip install .[dev]`
- `pytest -q`
- `pip-audit --desc`
- `bandit -q -r DBDuck -c .bandit`

Real backend integration tests are available under `tests/integration` for:

- `mongodb`
- `mysql`
- `postgresql`
- `mssql`

They are opt-in via environment flags so the default suite stays local and deterministic.
The integration suite now covers CRUD, transaction commit/rollback, native pagination, and connection-failure mapping for the current production backends.

## SQL Migration Baseline

Alembic baseline scaffold is included:

- `migrations/sql/alembic.ini`
- `migrations/sql/env.py`
- `migrations/sql/versions/`

Usage:

```bash
alembic -c migrations/sql/alembic.ini revision -m "init"
alembic -c migrations/sql/alembic.ini upgrade head
```

## Mongo Indexes

```python
db.ensure_indexes(
    "events",
    [
        {
            "fields": [{"name": "type", "order": "asc"}, {"name": "ts", "order": "desc"}],
            "options": {"name": "idx_type_ts"},
        }
    ],
)
```

Model-driven indexes:

```python
from DBDuck.udom.models.umodel import UModel

class Event(UModel):
    __collection__ = "events"
    __indexes__ = [
        {"fields": [{"name": "type", "order": "asc"}], "options": {"name": "idx_type"}},
    ]

Event.bind(db)
Event.ensure_indexes()
```

## Production Readiness Snapshot

- Current readiness estimate for current real backends: **88%**
- Coverage now includes robust SQL + Mongo core operations, security controls, and real backend integration scaffolding for `MySQL`, `PostgreSQL`, `SQL Server`, and `MongoDB`.
- Remaining work for higher confidence:
  - Migrations and schema evolution strategy.
  - Full real-backend integration execution in CI infrastructure.
  - Observability dashboards/alerts and SLOs.
  - Performance/load testing with real infra.
  - Release/versioning policy and backend compatibility matrix.

## Initialize Guide

See `docs/INITIALIZE.md` for full initialization steps.

## Logo
<p align="center">
  <img src="docs/assets/dbduck-logo.png" alt="DBDuck Logo" width="320" bg="black" />
</p>