# Changelog

## v0.3.0 - 2026-03-28

### Added
- **Query Builder DSL**: Fluent chainable API for constructing queries with method chaining.
  - `db.table("users")` returns a `QueryBuilder` for fluent operations.
  - **Works across all backends**: SQL (SQLite, MySQL, PostgreSQL, SQL Server), NoSQL (MongoDB), Graph (Neo4j), and Vector (Qdrant).
  - Chainable methods: `where()`, `order()`, `limit()`, `offset()`, `select()`, `page()`.
  - Comparison operators: `where_gt()`, `where_gte()`, `where_lt()`, `where_lte()`, `where_in()`, `where_not()`, `where_like()`, `where_null()`, `where_not_null()`.
  - OR conditions: `where_or()` for combining multiple condition groups.
  - Aggregation: `group_by()`, `having()`, `metrics()` for builder-style aggregation.
  - Terminal methods: `find()`, `first()`, `count()`, `exists()`, `update()`, `delete()`, `create()`, `create_many()`, `aggregate()`, `find_page()`.
  - Utility methods: `clone()` for reusable base queries, `to_dict()` for query introspection.
  - **Vector-specific**: `search_similar()`, `upsert_vector()` for similarity search and vector operations.
  - **Graph-specific**: `find_related()`, `create_relationship()` for traversing and creating relationships.
- **UModel Query Builder**: `Model.query()` returns a `ModelQueryBuilder` that provides the same fluent API but returns typed model instances instead of dictionaries.
  - `User.query().where(active=True).find()` returns `list[User]`
  - `User.query().where(id=1).first()` returns `User | None`
  - All chainable methods and terminal methods supported.
- Exported `QueryBuilder` class from `DBDuck` package.
- Comprehensive test suite for Query Builder (`tests/test_query_builder.py`).

### Example
```python
# UDOM Query Builder
db.table("users").where(active=True).order("name").limit(10).find()

# UModel Query Builder (returns typed instances)
User.query().where(active=True).order("name").find()  # list[User]
User.query().where(id=1).first()                      # User | None
```

## v0.2.1 - 2026-03-25

### Fixed
- Minor bug fixes and stability improvements.

## v0.2.0 - 2026-03-20

### Added
- `AsyncUDOM` with awaitable CRUD, pagination, aggregation, transaction, and lifecycle methods.
- Real Qdrant-backed vector adapter with collection management, vector upsert, similarity search, and count/delete support.
- Real Neo4j-capable graph adapter with parameterized Cypher for CRUD, relationships, related-node lookups, and shortest-path queries.
- `dbduck` CLI with `ping`, `shell`, `inspect`, `migrate`, and `version` commands.
- Async and vector test suites.

### Changed
- Made SQL setup more developer-friendly by allowing URL-only backend inference in the Python API and CLI.
- Added support for common legacy/current SQL aliases such as `psql`, `pg`, `postgresql`, and `sqlserver`.
- Improved CLI exception handling so `ping`, `inspect`, and shell flows return DBDuck-style masked errors instead of raw SQLAlchemy tracebacks.
- Added friendlier CLI hints for common connection failures like missing databases, auth failures, and unreachable hosts.
- Made CLI output quieter by default and added colorized status/error output with `colorama`.
- Hardened SQLAlchemy UQL translation so `CREATE`, `FIND`, and `DELETE` use parameterized execution paths.
- Removed the unsafe `allow_unsafe_where_strings` bypass path.
- Hardened MSSQL existence checks with parameterized `OBJECT_ID` lookups.
- Hardened legacy SQL adapters to use parameterized conditions and masked execution errors.
- Hardened graph UQL parsing and condition conversion to reject injected labels, field names, and raw Cypher fragments.
- Hardened UQL parser key/value parsing and malformed query handling.
- Hardened Mongo operator detection and blocked `$` operator values.
- Tightened BCrypt hash detection to require valid BCrypt length.
- Strengthened rate limiting to block direct writes to the audit entity and support per-caller buckets.
- Added pagination offset caps and UQL literal sanitization for large-offset and control-character abuse.

### Security
- Closed BUG-1 through BUG-28 from the injection hardening audit.
- Added a dedicated injection regression suite covering every listed vulnerability.
- Preserved masked public errors while keeping internal debug logs for maintainers.

### Packaging
- Bumped package version to `0.2.0`.
- Added PyPI publish workflow and `dbduck` console script.
- Added optional extras for MongoDB, async backends, vector support, graph support, and SQL Server support.

