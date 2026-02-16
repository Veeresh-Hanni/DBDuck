# dbduck (UDOM Core)

**dbduck is a universal data object model for SQL, NoSQL, graph, vector, and AI databases.**

Define data once, query anywhere.

## What It Does

`dbduck` (implemented here as `UDOM`) provides one consistent interface for working across multiple database types:

- SQL: SQLite, MySQL, PostgreSQL, MariaDB
- NoSQL: MongoDB and document-style stores
- Graph: Neo4j and graph backends
- Extensible path for vector and AI-native data workflows

## Core Interface

- `query(sql)`: run native queries on supported backends
- `uexecute(uql)`: run universal query language (UQL) operations

## Install

```bash
pip install -r requirements.txt
```

## Quick Example

```python
from udom import UDOM

# MySQL example

db = UDOM(
    db_type="mysql",
    url="mysql+pymysql://root:password@localhost:3306/udom"
)

# Native query
print(db.query("SELECT * FROM `User`;"))

# UQL create and read

db.uexecute('CREATE User {name: "Veeresh", age: 23, active: true}')
print(db.uexecute("FIND User WHERE age > 21"))
```

More examples are available in `examples/`.
