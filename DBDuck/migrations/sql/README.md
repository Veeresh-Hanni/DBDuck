# SQL Migration Baseline (Alembic)

This folder provides the initial migration baseline for SQL backends.

## Setup

1. Install dev dependencies:

```bash
pip install .[dev]
```

2. Export database URL:

```bash
# PowerShell
$env:DATABASE_URL="sqlite:///test.db"
```

3. Create a revision:

```bash
dbduck makemigrations --module myapp.models --message "init"
```

4. Apply migrations:

```bash
dbduck migrate --direction up
```

## Notes

- `DATABASE_URL` or `DBDUCK_DATABASE_URL` is required for migration execution.
- `DBDUCK_MODEL_MODULE` is used during `makemigrations` to load `UModel` classes and build Alembic metadata.
- `dbduck makemigrations` adds the current working directory to imports by default. Run it from your project root, or pass `--project-dir` explicitly if your models live elsewhere.
- `dbduck migrate` also runs with the project directory as its working directory, so relative SQLite files are created inside the user project.
- If `migrations/sql` does not exist in the project yet, DBDuck creates it automatically on the first `makemigrations` or `migrate` call.
- Current UDOM SQL path supports dynamic table creation. This baseline exists to
  transition toward explicit schema-managed production deployments.