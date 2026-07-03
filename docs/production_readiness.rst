Production Readiness
====================

DBDuck provides tested core paths for SQLite, MySQL, PostgreSQL, SQL Server,
MongoDB, Neo4j, and Qdrant. SQLite tests run in the default CI suite; live
backend integration tests are opt-in and require their respective services.

Before production deployment
----------------------------

- Pin DBDuck and database-driver versions.
- Store connection strings in a secrets manager.
- Run migrations against a staging copy and back up production data.
- Enable integration tests for every backend used by the application.
- Configure connection-pool limits, timeouts, structured logs, and monitoring.
- Exercise restore and rollback procedures before a release.

Release checks
--------------

The project CI runs the Python 3.10--3.12 test matrix, Bandit, dependency
auditing, and package validation. Tagged releases build distributions and
publish release artifacts after the tag is checked against the package version.
