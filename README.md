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

sql = UDOM(url="postgresql+psycopg2://...")
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

db = UDOM(url="sqlite:///app.db")
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
    db = AsyncUDOM(url="postgresql+psycopg2://postgres:pass@localhost:5432/app")
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

## Developer-friendly setup
- For SQL backends, DBDuck can infer the backend directly from the URL:
  - `UDOM(url="sqlite:///app.db")`
  - `UDOM(url="mysql+pymysql://root:pass@localhost:3306/app")`
  - `UDOM(url="postgresql+psycopg2://postgres:pass@localhost:5432/app")`
  - `UDOM(url="mssql+pyodbc:///?odbc_connect=...")`
- Explicit configuration still works if you prefer it:
  - `UDOM(db_type="sql", db_instance="postgres", url="...")`
- Common SQL aliases are accepted:
  - `postgres`, `postgresql`, `psql`, `pg`
  - `mssql`, `sqlserver`

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
- `table(entity)`: returns a fluent QueryBuilder for chainable queries.

Full docs live in the codebase docstrings and examples.

## Query Builder DSL

DBDuck 0.3.0 introduces a fluent Query Builder API for constructing queries with method chaining:

```python
from DBDuck import UDOM

db = UDOM(url="sqlite:///app.db")

# Fluent query building
users = db.table("users").where(active=True).order("name").limit(10).find()
user = db.table("users").where(id=1).first()
count = db.table("users").where(role="admin").count()

# Field projection
db.table("users").select("id", "name", "email").find()

# Pagination
db.table("users").page(2, 25).find()
db.table("users").find_page(page=2, page_size=25)

# Comparison operators
db.table("users").where_gt(age=18).find()           # age > 18
db.table("users").where_gte(age=21).find()          # age >= 21
db.table("users").where_lt(age=65).find()           # age < 65
db.table("users").where_in("role", ["admin", "mod"]).find()

# Mutations
db.table("users").where(id=1).update({"name": "Updated"})
db.table("users").where(id=1).delete()
db.table("users").create({"name": "New User", "email": "new@example.com"})

# Aggregation
db.table("orders").group_by("status").metrics(total="count").aggregate()

# Check existence
if db.table("users").where(email="test@example.com").exists():
    print("User exists")

# Clone for reuse
base = db.table("users").where(active=True)
admins = base.clone().where(role="admin").find()
regular = base.clone().where(role="user").find()
```

### Query Builder Across All Backends

The Query Builder works uniformly across all database types:

```python
# SQL (SQLite, MySQL, PostgreSQL, SQL Server)
sql_db = UDOM(url="postgresql+psycopg2://postgres:pass@localhost/app")
sql_db.table("users").where(active=True).order("name").limit(10).find()

# NoSQL (MongoDB)
mongo_db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/app")
mongo_db.table("profiles").where(active=True).find()
mongo_db.table("orders").group_by("status").metrics(total="count").aggregate()

# Graph (Neo4j)
graph_db = UDOM(db_type="graph", db_instance="neo4j", url="bolt://localhost:7687")
graph_db.table("User").where(name="Alice").find()
graph_db.table("User").find_related(id="u1", rel_type="FOLLOWS", direction="out")
graph_db.table("User").create_relationship("u1", "FOLLOWS", "User", "u2")

# Vector (Qdrant)
vector_db = UDOM(db_type="vector", db_instance="qdrant", url="http://localhost:6333")
vector_db.table("embeddings").where(category="tech").search_similar([0.1, 0.2, 0.3], top_k=5)
vector_db.table("embeddings").upsert_vector("v1", [0.1, 0.2, 0.3], {"label": "test"})
```

## CLI
```bash
dbduck ping --url "postgresql+psycopg2://postgres:pass@localhost:5432/app"
dbduck shell --url "sqlite:///app.db"
dbduck inspect --url "sqlite:///app.db" --entity users
dbduck version
```

For SQL backends, `dbduck` can infer the backend from the URL, so `--type` and `--instance` are optional.
CLI output is quiet by default and colorized for easier scanning in the terminal.

## UModel
```python
from DBDuck import UDOM, UModel

class User(UModel):
    __entity__ = "users"
    __sensitive_fields__ = ["password"]
    id: int
    email: str
    password: str

User.bind(UDOM(url="sqlite:///app.db"))
user = User(id=1, email="user@example.com", password="plain-text")
user.save()
print(User.find_one(where={"id": 1}).to_dict())
print(User.find_one(where={"id": 1}).verify_secret("password", "plain-text"))
```

### UModel with Query Builder

UModel also supports the fluent Query Builder via `Model.query()`:

```python
from DBDuck import UDOM, UModel

class User(UModel):
    __entity__ = "users"
    id: int
    name: str
    role: str
    active: bool

User.bind(UDOM(url="sqlite:///app.db"))

# Fluent queries returning typed model instances
users = User.query().where(active=True).order("name").find()  # list[User]
user = User.query().where(id=1).first()                       # User | None
count = User.query().where(role="admin").count()              # int

# Chaining with comparison operators
adults = User.query().where_gte(age=18).where_lt(age=65).find()

# Clone for reusable base queries
active = User.query().where(active=True)
admins = active.clone().where(role="admin").find()
users = active.clone().where(role="user").find()

# Mutations
User.query().where(id=1).update({"name": "Updated"})
User.query().where(id=1).delete()

# Pagination with model instances
page = User.query().find_page(page=2, page_size=25)
for user in page["items"]:  # Each item is a User instance
    print(user.name)
```

## Errors
- DBDuck maps backend failures to DBDuck exceptions:
  - `ConnectionError`
  - `QueryError`
  - `TransactionError`
- CLI commands return masked, developer-friendly messages instead of raw SQLAlchemy tracebacks.
- Use `--debug-errors` in `dbduck shell` if you want the full underlying traceback while debugging locally.

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

