# DBDuck Initialization Guide

This guide is for the current production-focused stage of DBDuck with SQL and NoSQL support.

## 1. Environment

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

## 2. Local Source Priority

When running from `examples/`, keep local source first:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
```

## 3. SQL Initialization

### SQLite

```python
from DBDuck import UDOM

db = UDOM(db_type="sql", db_instance="sqlite", url="sqlite:///test.db")
print(db.create("Product", {"name": "Keyboard", "price": 99, "active": True}))
print(db.find("Product", where={"active": True}))
```

### MySQL

```python
db = UDOM(db_type="sql", db_instance="mysql", url="mysql+pymysql://username:pass@localhost:3306/udom")
```

### PostgreSQL

```python
db = UDOM(db_type="sql", db_instance="postgres", url="postgresql+psycopg2://username:pass@localhost:5432/postgres")
```

### Supported SQL engines in current workspace

- `sqlite`
- `mysql`
- `postgres`
- `mssql`

## 4. NoSQL Initialization (MongoDB)

```python
db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
print(db.execute("ping"))
print(db.create("events", {"type": "login", "ok": True}))
print(db.find("events", where={"ok": True}))
```

### Mongo Transactions

```python
with db.transaction():
    db.create("events", {"type": "purchase", "ok": True, "amount": 120.50})
```

### Mongo Index Management

```python
db.ensure_indexes(
    "events",
    [
        {"fields": [{"name": "type", "order": "asc"}], "options": {"name": "idx_type"}},
    ],
)
```

## 5. Validation Commands

```bash
python -m py_compile DBDuck/udom/udom.py
python examples/app_production.py
python examples/example_sqlite.py
python examples/example_mongo.py
python -m examples.dbs.sqlite.basic
python -m examples.dbs.sqlite.advanced
```

## 6. Current Scope

- Production focus: SQL + MongoDB
- In progress: Graph + AI + Vector

## 7. CI Test Pipeline

GitHub Actions workflow:

- `.github/workflows/ci.yml`

Local equivalent:

```bash
pytest -q
```

Security checks:

```bash
pip-audit --desc
bandit -q -r DBDuck
```

## 8. Runtime Config

Use `.env.example` as baseline for production environment variables.

Important secure default:


## 9. SQL Migrations

Use Alembic baseline in `migrations/sql/`:

```bash
$env:DATABASE_URL="sqlite:///test.db"
dbduck makemigrations --module myapp.models --message "init"
dbduck migrate --direction up
```

## 10. Mongo Integration Tests

```bash
$env:RUN_MONGO_INTEGRATION="1"
$env:MONGO_TEST_URL="mongodb://localhost:27017/udom_test"
pytest -q tests/integration
```

## 11. Logo Asset

Expected logo location:

- `docs/assets/dbduck-logo.png`
