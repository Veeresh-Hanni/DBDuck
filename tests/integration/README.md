# Integration Tests

These tests are designed for real backend deployments.

## Run

```bash
# PowerShell
$env:RUN_MONGO_INTEGRATION="1"
$env:MONGO_TEST_URL="mongodb://localhost:27017/udom_test"
$env:RUN_MONGO_TX_INTEGRATION="1"  # only if using replica set transactions
pytest -q tests/integration
```

SQL backends:

```bash
# MySQL
$env:RUN_MYSQL_INTEGRATION="1"
$env:MYSQL_TEST_URL="mysql+pymysql://root:password@localhost:3306/udom_test"

# PostgreSQL
$env:RUN_POSTGRES_INTEGRATION="1"
$env:POSTGRES_TEST_URL="postgresql+psycopg2://postgres:password@localhost:5432/udom_test"

# SQL Server
$env:RUN_MSSQL_INTEGRATION="1"
$env:MSSQL_TEST_URL="mssql+pyodbc://sa:Password!123@localhost:1433/udom_test?driver=ODBC+Driver+17+for+SQL+Server"

pytest -q tests/integration
```

## Notes

- Tests are skipped unless their `RUN_*_INTEGRATION=1` flag is set.
- Use isolated test database and disposable data.
- SQL tests cover `ping`, CRUD roundtrip, transaction commit/rollback, native pagination, and connection-failure mapping for MySQL, PostgreSQL, and SQL Server.
- SQL migration tests cover `dbduck makemigrations` + `dbduck migrate --direction up` for SQLite, and for MySQL/PostgreSQL/SQL Server when their integration flags are enabled.
- Mongo tests cover `ping`, CRUD roundtrip, native pagination, connection-failure mapping, and optional transaction rollback with isolated collection names.
