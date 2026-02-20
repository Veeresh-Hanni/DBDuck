# DBDuck Initialization Guide

This guide is for the initial stage of DBDuck with SQL and NoSQL support.

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
db = UDOM(db_type="sql", db_instance="mysql", url="mysql+pymysql://root:password@localhost:3306/udom")
```

### PostgreSQL

```python
db = UDOM(db_type="sql", db_instance="postgres", url="postgresql+psycopg2://postgres:password@localhost:5432/postgres")
```

### MSSQL

```python
from urllib.parse import quote_plus

odbc = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=HOST\\INSTANCE;DATABASE=udom;Trusted_Connection=yes;"
url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc)
db = UDOM(db_type="sql", db_instance="mssql", url=url)
```

## 4. NoSQL Initialization (MongoDB)

```python
db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/udom")
print(db.execute("ping"))
print(db.create("events", {"type": "login", "ok": True}))
print(db.find("events", where={"ok": True}))
```

## 5. Validation Commands

```bash
python -m py_compile DBDuck/udom/udom.py
python examples/example_sqlite.py
python examples/example_mongo.py
```

## 6. Current Scope

- Production focus: SQL + MongoDB
- In progress: Graph + AI + Vector

## 7. Logo Asset

Expected logo location:

- `docs/assets/dbduck-logo.png`
