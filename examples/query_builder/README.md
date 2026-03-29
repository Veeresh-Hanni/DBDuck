# Query Builder Examples

This folder contains examples demonstrating the Query Builder DSL introduced in DBDuck v0.3.0.

## Examples

| File | Description |
|------|-------------|
| `example_sql_query_builder.py` | Basic Query Builder with SQLite |
| `example_sql_joins.py` | SQL join examples with inner join and left join |
| `example_umodel_query_builder.py` | UModel with Django-style column definitions |
| `example_all_backends.py` | Query Builder across SQL, MongoDB, Neo4j, Qdrant |
| `example_all_sql_backends.py` | All SQL backends (SQLite, MySQL, PostgreSQL, SQL Server) |
| `example_model_relationships.py` | Model relationships (ForeignKey, OneToMany, ManyToMany) |

## Running Examples

```bash
# SQL Query Builder (no setup required)
python example_sql_query_builder.py

# SQL joins (no setup required)
python example_sql_joins.py

# UModel Query Builder with typed results
python example_umodel_query_builder.py

# Model relationships example
python example_model_relationships.py

# All SQL backends (MySQL, PostgreSQL, SQL Server need servers)
python example_all_sql_backends.py

# All backends (requires MongoDB, Neo4j, Qdrant running)
python example_all_backends.py
```

## Query Builder API

### Basic API (returns dicts)
```python
from DBDuck import UDOM

db = UDOM(url="sqlite:///:memory:")

results = db.table("users") \
    .where(active=True) \
    .order("name") \
    .limit(10) \
    .find()
```

### UModel API (returns typed instances)
```python
from DBDuck.models import (
    CharField, IntegerField, BooleanField,
    Column, ForeignKey, UModel, CASCADE
)

class User(UModel):
    __entity__ = "users"
    
    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    active = Column(BooleanField, default=True)

User.bind(db)

# Returns User instances, not dicts
users = User.query().where(active=True).find()
```

## Model Definitions

```python
from DBDuck.models import (
    BooleanField, CharField, Column, ForeignKey,
    IntegerField, OneToMany, UModel, CASCADE
)

class Author(UModel):
    __entity__ = "authors"
    
    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    books = OneToMany("Book", back_populates="author")

class Book(UModel):
    __entity__ = "books"
    
    id = Column(IntegerField, primary_key=True)
    title = Column(CharField, nullable=False)
    author_id = ForeignKey(Author, on_delete=CASCADE)
```
