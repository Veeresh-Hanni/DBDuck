# DBDuck

<p align="center">
  <img src="docs/assets/dbduck-logo.png" alt="DBDuck Logo" width="320" bg="black" />
</p>

**Universal Data Object Model (UDOM) for SQL and NoSQL.**

DBDuck gives one API for data operations across engines.

## Current Stage

- Stable focus: `SQL` + `NoSQL (MongoDB)`
- Next phase: Graph, AI, Vector

## Install

```bash
pip install .
# for tests and tooling
pip install .[dev]
```

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
```

## Core API

- `create(entity, data)`
- `create_many(entity, rows)`
- `find(entity, where=None, order_by=None, limit=None)`
- `delete(entity, where)`
- `execute(native_query)`
- `uquery(uql)`
- `uexecute(uql)`
- `begin()`
- `commit()`
- `rollback()`
- `transaction()`

## Production Architecture

```text
DBDuck/
  core/
    base_adapter.py
    connection_manager.py
    exceptions.py
    transaction.py
  adapters/
    mysql_adapter.py
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
- `ConnectionManager` provides lazy, thread-safe engine/session reuse.
- Structured logging captures query, error, and connection events.

## Initialize Guide

See `docs/INITIALIZE.md` for full initialization steps.

## Logo

Place your logo file here:

- `docs/assets/dbduck-logo.png`
