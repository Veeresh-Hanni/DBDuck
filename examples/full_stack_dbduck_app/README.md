# DBDuck Full Stack FastAPI Showcase

This is a complete example app using the latest DBDuck features in one place, organized as a modular FastAPI package.

What it demonstrates:

- `UDOM` with URL-first setup
- Django-style `UModel` imports from `DBDuck.models`
- `ManyToOne`, `OneToMany`, `OneToOne`, `ManyToMany`
- auto-hashing of sensitive fields
- Query Builder SQL joins
- UQL via `uquery()` and `uexecute()`
- aggregate + pagination through the latest query builder APIs
- FastAPI routes + a browser UI

## Run

```bash
uvicorn examples.full_stack_dbduck_app.main:app --reload
```

Open:

- `http://127.0.0.1:8000/`

## Optional environment variables

```bash
APP_DB_URL=sqlite:///dbduck_showcase.db
APP_LOG_LEVEL=ERROR
```

## API routes

- `GET /` - browser UI
- `GET /api/health`
- `GET /api/dashboard`
- `GET /api/customers`
- `POST /api/customers`
- `GET /api/products`
- `POST /api/products`
- `GET /api/orders`
- `GET /api/orders/joined`
- `POST /api/orders`
- `GET /api/stats`
- `GET /api/uql/sample`

## Notes

- SQLite is the default backend, so the example runs without extra services.
- Customer passwords are hashed automatically through `__sensitive_fields__`.
- Joined orders use the new SQL `join()` support in `QueryBuilder`.
- Reads, counts, pagination, joins, and aggregates use the current query builder style.
- The app is split into:
  - `main.py` - app entrypoint
  - `core/` - configuration helpers
  - `db/` - database bootstrap and seeding
  - `models.py` - UModel definitions
  - `schemas/` - request schemas by domain
  - `services/` - business logic by domain
  - `api/` - API routes and dependencies
  - `web/` - browser routes and HTML view
