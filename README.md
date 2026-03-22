# DBDuck — One API for every database

[![PyPI version](https://img.shields.io/pypi/v/dbduck.svg)](https://pypi.org/project/dbduck/)
[![Python versions](https://img.shields.io/pypi/pyversions/dbduck.svg)](https://pypi.org/project/dbduck/)
[![CI](https://img.shields.io/github/actions/workflow/status/Veeresh-Hanni/DBDuck/ci.yml?branch=main)](https://github.com/Veeresh-Hanni/DBDuck/actions)
[![License](https://img.shields.io/github/license/Veeresh-Hanni/DBDuck)](LICENSE)

<p align="center">
  <img src="docs/assets/dbduck-logo.png" alt="DBDuck Logo" bg="black" />
</p>

DBDuck is a Universal Data Object Model (UDOM): one Python API for SQL, MongoDB, Neo4j, Qdrant, and async data workflows.

## The problem
You use Postgres and MongoDB and Qdrant and Neo4j.
That means four clients, four query styles, four error formats, and four security surfaces.
Every feature team ends up rebuilding the same validation, retries, logging, and model plumbing.
The more backends you add, the more your application code turns into adapter glue.

## The solution
```python
# BEFORE: four clients, four mental models
import asyncpg
from pymongo import MongoClient
from neo4j import GraphDatabase
from qdrant_client import QdrantClient

# 40+ lines of setup, auth, query translation, and result normalization
```

```python
# AFTER: one API, one model, one error surface
from DBDuck import UDOM

sql = UDOM(db_type="sql", db_instance="postgres", url="postgresql+psycopg2://...")
mongo = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/app")
graph = UDOM(db_type="graph", db_instance="neo4j", url="bolt://localhost:7687")
vector = UDOM(db_type="vector", db_instance="qdrant", url="http://localhost:6333")

orders = sql.find("orders", where={"paid": True})
profiles = mongo.find("profiles", where={"active": True})
related = graph.find_related("User", id="u1", rel_type="PURCHASED")
nearest = vector.search_similar("products", vector=[0.1, 0.2, 0.3], top_k=5)
```

## Install
```bash
pip install dbduck
pip install dbduck[mongo]    # MongoDB support
pip install dbduck[async]    # AsyncUDOM
pip install dbduck[vector]   # Vector DB (Qdrant)
pip install dbduck[graph]    # Neo4j
pip install dbduck[all]      # Everything
```

## Quick start

### SQLite
```python
from DBDuck import UDOM

db = UDOM(db_type="sql", db_instance="sqlite", url="sqlite:///app.db")
db.create("users", {"id": 1, "name": "Asha", "active": True})
users = db.find("users", where={"active": True})
print(users)
```

### MongoDB
```python
from DBDuck import UDOM

db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/app")
db.create("profiles", {"id": "p1", "name": "Nila", "active": True})
profiles = db.find("profiles", where={"active": True})
print(profiles)
```

### Async Postgres
```python
import asyncio
from DBDuck.udom.async_udom import AsyncUDOM

async def main():
    db = AsyncUDOM(db_type="sql", db_instance="postgres", url="postgresql+psycopg2://postgres:pass@localhost:5432/app")
    await db.create("users", {"id": 1, "name": "Ishan", "active": True})
    print(await db.find("users", where={"active": True}))
    await db.close()

asyncio.run(main())
```

### Qdrant
```python
from DBDuck import UDOM

db = UDOM(db_type="vector", db_instance="qdrant", url="http://localhost:6333")
db.create_collection("products", vector_size=3, distance="cosine")
db.upsert_vector("products", id="p1", vector=[0.1, 0.2, 0.3], metadata={"name": "Widget"})
print(db.search_similar("products", vector=[0.1, 0.2, 0.3], top_k=3))
```

### Neo4j
```python
from DBDuck import UDOM

db = UDOM(db_type="graph", db_instance="neo4j", url="bolt://localhost:7687", auth=("neo4j", "password"))
db.create("User", {"id": "u1", "name": "Mira"})
db.create("Company", {"id": "c1", "name": "DBDuck"})
db.create_relationship("User", "u1", "WORKS_AT", "Company", "c1", {"role": "Engineer"})
print(db.find_related("User", id="u1", rel_type="WORKS_AT", target_label="Company"))
```

## Supported backends

| Backend | Type | Status | Install extra |
| --- | --- | --- | --- |
| SQLite | SQL | Production-capable | base |
| MySQL | SQL | Production-capable | base driver required |
| PostgreSQL | SQL | Production-capable | base driver required |
| SQL Server | SQL | Production-capable | `mssql` |
| MongoDB | NoSQL | Production-capable | `mongo` |
| Neo4j | Graph | Production-capable | `graph` |
| Qdrant | Vector | Production-capable | `vector` |
| Pinecone | Vector | Stub/TODO | planned |
| Weaviate | Vector | Stub/TODO | planned |
| Chroma | Vector | Stub/TODO | planned |
| AI backends | AI | Experimental pass-through | planned |

## Core API reference
- `create(entity, data)`: insert one record, document, node, or vector payload.
- `create_many(entity, rows)`: batch insert records or documents.
- `find(entity, where=None, order_by=None, limit=None)`: fetch matching records.
- `find_page(entity, page=1, page_size=20, where=None, order_by=None)`: offset pagination with safety caps.
- `update(entity, data, where)`: update matching records safely.
- `delete(entity, where)`: delete matching records safely.
- `count(entity, where=None)`: count matching records.
- `aggregate(...)`: backend-aware aggregation for SQL and MongoDB.
- `begin() / commit() / rollback() / transaction()`: transaction control.
- `ping() / close()`: lifecycle and health checks.
- `uexecute(uql)`: execute UQL through backend-specific parameterized translation.
- `create_relationship(...) / find_related(...) / shortest_path(...)`: graph-specific helpers.
- `create_collection(...) / upsert_vector(...) / search_similar(...)`: vector-specific helpers.

Full docs live in the codebase docstrings and examples.

## UModel
```python
from DBDuck import UDOM, UModel

class User(UModel):
    __entity__ = "users"
    __sensitive_fields__ = ["password"]
    id: int
    email: str
    password: str

User.bind(UDOM(db_type="sql", db_instance="sqlite", url="sqlite:///app.db"))
user = User(id=1, email="user@example.com", password="plain-text")
user.save()
print(User.find_one(where={"id": 1}).to_dict())
print(User.find_one(where={"id": 1}).verify_secret("password", "plain-text"))
```

## Security
- Parameterized SQL and parameterized Cypher generation.
- UQL string hardening for `FIND`, `CREATE`, and `DELETE`.
- Mongo operator-injection blocking.
- Identifier validation across entities, fields, labels, and relationship types.
- BCrypt hashing for sensitive fields.
- `verify_secret()` helper for BCrypt validation.
- Structured logging without raw SQL or user secrets in normal logs.
- Custom exception hierarchy with masked execution errors.
- Security audit logging for blocked operations.
- Per-caller rate limiting support.

## Roadmap
DBDuck 0.2.0 delivers the hardened SQL core, Mongo support, Neo4j graph support, Qdrant vector support, AsyncUDOM, and the CLI.
Next up: deeper vector backends, richer schema migration workflows, Redis and DynamoDB adapters, and first-class observability hooks.

## Contributing
Issues, discussions, and pull requests are welcome.
See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.
