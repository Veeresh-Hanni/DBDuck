# Changelog

## v0.2.0 - 2026-03-20

### Added
- `AsyncUDOM` with awaitable CRUD, pagination, aggregation, transaction, and lifecycle methods.
- Real Qdrant-backed vector adapter with collection management, vector upsert, similarity search, and count/delete support.
- Real Neo4j-capable graph adapter with parameterized Cypher for CRUD, relationships, related-node lookups, and shortest-path queries.
- `dbduck` CLI with `ping`, `shell`, `inspect`, `migrate`, and `version` commands.
- Async and vector test suites.

### Changed
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

