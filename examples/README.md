# DBDuck Examples

This folder contains runnable examples from basic to advanced by database engine.

- `examples/app_production.py` (single app-style flow with strict `UModel`, health, transaction, CRUD, pagination)
- `examples/fastapi_dbduck_app/main.py` (production-style FastAPI service using DBDuck)
- `examples/fastapi_dbduck_app/README.md` (run and API usage guide)
- `examples/full_stack_dbduck_app/main.py` (complete FastAPI + browser showcase using UDOM, UModel, joins, UQL)
- `examples/full_stack_dbduck_app/README.md`
- `examples/query_builder/example_sql_query_builder.py`
- `examples/query_builder/example_sql_joins.py`
- `examples/query_builder/example_umodel_query_builder.py`
- `examples/dbs/sqlite/basic.py`
- `examples/dbs/sqlite/advanced.py`
- `examples/dbs/mysql/basic.py`
- `examples/dbs/mysql/advanced.py`
- `examples/dbs/postgres/basic.py`
- `examples/dbs/postgres/advanced.py`
- `examples/dbs/mssql/basic.py`
- `examples/dbs/mssql/advanced.py`
- `examples/dbs/mongodb/basic.py`
- `examples/dbs/mongodb/advanced.py`

Quick run examples:

```bash
python examples/app_production.py
uvicorn examples.full_stack_dbduck_app.main:app --reload
python examples/query_builder/example_sql_query_builder.py
python examples/query_builder/example_sql_joins.py
uvicorn examples.fastapi_dbduck_app.main:app --host 0.0.0.0 --port 8000 --workers 2
python -m examples.dbs.sqlite.basic
python -m examples.dbs.sqlite.advanced
```

For network databases (MySQL/Postgres/MSSQL/MongoDB), update connection URLs first.

`app_production.py` reads env vars:

- `APP_DB_TYPE` (default: `sql`)
- `APP_DB_INSTANCE` (default: `sqlite`)
- `APP_DB_URL` (default: `sqlite:///dbduck_app.db`)
- `APP_LOG_LEVEL` (default: `INFO`)

Notes:

- The production app uses strict `UModel` validation (`__strict__ = True`).
- SQL table creation remains automatic (`CREATE TABLE IF NOT EXISTS`) through DBDuck model save/bulk paths.
- The app performs idempotent seed cleanup so repeated runs stay stable.
