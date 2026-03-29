"""
Query Builder with All SQL Backends

Demonstrates the Query Builder DSL working with:
- SQLite (in-memory, no setup required)
- MySQL (requires running MySQL server)
- PostgreSQL (requires running PostgreSQL server)
- SQL Server (requires running SQL Server)

Each example uses Django-style UModel definitions.
"""

from urllib.parse import quote_plus

from DBDuck import UDOM
from DBDuck.models import (
    BooleanField,
    CharField,
    Column,
    ForeignKey,
    IntegerField,
    UModel,
    CASCADE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Model Definitions (shared across all backends)
# ─────────────────────────────────────────────────────────────────────────────

class Product(UModel):
    """Product model with Django-style columns."""
    __entity__ = "products"
    
    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    category = Column(CharField)
    price = Column(IntegerField)
    in_stock = Column(BooleanField, default=True)


class Category(UModel):
    """Category model."""
    __entity__ = "categories"
    
    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    description = Column(CharField, nullable=True)


# ─────────────────────────────────────────────────────────────────────────────
# SQLite Example (Always works - in-memory)
# ─────────────────────────────────────────────────────────────────────────────

def run_sqlite_example():
    """Query Builder with SQLite."""
    print("\n" + "=" * 60)
    print("SQLite - Query Builder Example")
    print("=" * 60)
    
    # Connect to in-memory SQLite
    db = UDOM(url="sqlite:///:memory:")
    
    # Create table
    db.adapter.run_native("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            price INTEGER,
            in_stock INTEGER DEFAULT 1
        )
    """)
    
    # Bind model
    Product.bind(db)
    
    # Insert data using Query Builder
    db.table("products").create({"id": 1, "name": "Laptop", "category": "electronics", "price": 999, "in_stock": 1})
    db.table("products").create({"id": 2, "name": "Mouse", "category": "electronics", "price": 29, "in_stock": 1})
    db.table("products").create({"id": 3, "name": "Desk", "category": "furniture", "price": 249, "in_stock": 1})
    db.table("products").create({"id": 4, "name": "Chair", "category": "furniture", "price": 149, "in_stock": 0})
    
    print("Inserted 4 products")
    
    # Query Builder operations
    all_products = db.table("products").find()
    print(f"All products: {len(all_products)}")
    
    electronics = db.table("products").where(category="electronics").find()
    print(f"Electronics: {[p['name'] for p in electronics]}")
    
    in_stock = db.table("products").where(in_stock=1).order("price", "DESC").find()
    print(f"In stock (by price): {[p['name'] for p in in_stock]}")
    
    # UModel Query Builder (returns typed instances)
    products = Product.query().where(in_stock=1).find()
    print(f"UModel results: {[p.name for p in products]}")
    
    first = Product.query().where(id=1).first()
    print(f"First product: {first.name} (${first.price})")
    
    count = Product.query().where(category="electronics").count()
    print(f"Electronics count: {count}")
    
    print("✓ SQLite Query Builder works!\n")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# MySQL Example
# ─────────────────────────────────────────────────────────────────────────────

def run_mysql_example():
    """Query Builder with MySQL."""
    print("\n" + "=" * 60)
    print("MySQL - Query Builder Example")
    print("=" * 60)
    
    try:
        # Connect to MySQL (adjust credentials as needed)
        db = UDOM(url="mysql+pymysql://root:Veeru123@localhost:3306/dbduck_test")
        db.ping()
    except Exception as e:
        print(f"MySQL not available: {e}")
        print("Skipping MySQL examples")
        print("To test: Start MySQL and create database 'dbduck_test'\n")
        return False
    
    # Create table
    try:
        db.adapter.run_native("DROP TABLE IF EXISTS products")
        db.adapter.run_native("""
            CREATE TABLE products (
                id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                price INT,
                in_stock BOOLEAN DEFAULT TRUE
            )
        """)
    except Exception as e:
        print(f"Table creation failed: {e}")
        return False
    
    # Bind model
    Product.bind(db)
    
    # Insert data
    db.table("products").create({"id": 1, "name": "Laptop", "category": "electronics", "price": 999, "in_stock": True})
    db.table("products").create({"id": 2, "name": "Phone", "category": "electronics", "price": 599, "in_stock": True})
    db.table("products").create({"id": 3, "name": "Tablet", "category": "electronics", "price": 399, "in_stock": False})
    
    print("Inserted 3 products")
    
    # Query Builder
    all_products = db.table("products").find()
    print(f"All products: {len(all_products)}")
    
    # UModel Query Builder
    products = Product.query().where(in_stock=True).find()
    print(f"In stock: {[p.name for p in products]}")
    
    # Pagination
    page = Product.query().find_page(page=1, page_size=2)
    print(f"Page 1: {[p.name for p in page['items']]}")
    
    print("✓ MySQL Query Builder works!\n")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# PostgreSQL Example
# ─────────────────────────────────────────────────────────────────────────────

def run_postgresql_example():
    """Query Builder with PostgreSQL."""
    print("\n" + "=" * 60)
    print("PostgreSQL - Query Builder Example")
    print("=" * 60)
    
    try:
        # Connect to PostgreSQL (adjust credentials as needed)
        db = UDOM(url="postgresql+psycopg2://postgres:Veeru123@localhost:5432/dbduck_test")
        db.ping()
    except Exception as e:
        print(f"PostgreSQL not available: {e}")
        print("Skipping PostgreSQL examples")
        print("To test: Start PostgreSQL and create database 'dbduck_test'\n")
        return False
    
    # Create table
    try:
        db.adapter.run_native("DROP TABLE IF EXISTS products CASCADE")
        db.adapter.run_native("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                category VARCHAR(100),
                price INTEGER,
                in_stock BOOLEAN DEFAULT TRUE
            )
        """)
    except Exception as e:
        print(f"Table creation failed: {e}")
        return False
    
    # Bind model
    Product.bind(db)
    
    # Insert data
    db.table("products").create({"id": 1, "name": "Server", "category": "hardware", "price": 2999, "in_stock": True})
    db.table("products").create({"id": 2, "name": "Router", "category": "network", "price": 199, "in_stock": True})
    db.table("products").create({"id": 3, "name": "Switch", "category": "network", "price": 99, "in_stock": False})
    
    print("Inserted 3 products")
    
    # Query Builder
    network = db.table("products").where(category="network").find()
    print(f"Network products: {[p['name'] for p in network]}")
    
    # UModel Query Builder with chaining
    products = (
        Product.query()
        .where(in_stock=True)
        .order("price", "DESC")
        .find()
    )
    print(f"In stock (by price): {[f'{p.name} (${p.price})' for p in products]}")
    
    # Update via Query Builder
    Product.query().where(id=3).update({"in_stock": True})
    updated = Product.query().where(id=3).first()
    print(f"Updated Switch in_stock: {updated.in_stock}")
    
    print("✓ PostgreSQL Query Builder works!\n")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# SQL Server Example
# ─────────────────────────────────────────────────────────────────────────────

def run_mssql_example():
    """Query Builder with SQL Server."""
    print("\n" + "=" * 60)
    print("SQL Server - Query Builder Example")
    print("=" * 60)
    odbc_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=VEERESH-HANNI\MSSQLLEARNDB"
        "DATABASE=dbduck_test;"
        "UID=sa;"
        "PWD=Veeru2006;"
        "TrustServerCertificate=yes;"
    )
    url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
    # url = "mssql+pyodbc://sa:Veeru2006@localhost:1433/dbduck_test?driver=ODBC+Driver+17+for+SQL+Server"
    try:
        # Connect to SQL Server (adjust connection string as needed)
        
        db = UDOM(
            url=url
        )
        db.ping()
    except Exception as e:
        print(f"SQL Server not available: {e}")
        print("Skipping SQL Server examples")
        print("To test: Start SQL Server and create database 'dbduck_test'\n")
        return False
    
    # Create table
    try:
        db.adapter.run_native("""
            IF OBJECT_ID('products', 'U') IS NOT NULL DROP TABLE products
        """)
        db.adapter.run_native("""
            CREATE TABLE products (
                id INT PRIMARY KEY,
                name NVARCHAR(255) NOT NULL,
                category NVARCHAR(100),
                price INT,
                in_stock BIT DEFAULT 1
            )
        """)
    except Exception as e:
        print(f"Table creation failed: {e}")
        return False
    
    # Bind model
    Product.bind(db)
    
    # Insert data
    db.table("products").create({"id": 1, "name": "Windows Server", "category": "software", "price": 999, "in_stock": 1})
    db.table("products").create({"id": 2, "name": "SQL Server", "category": "software", "price": 1999, "in_stock": 1})
    db.table("products").create({"id": 3, "name": "Azure VM", "category": "cloud", "price": 499, "in_stock": 1})
    
    print("Inserted 3 products")
    
    # Query Builder
    software = db.table("products").where(category="software").find()
    print(f"Software products: {[p['name'] for p in software]}")
    
    # UModel Query Builder
    products = Product.query().order("price", "DESC").limit(2).find()
    print(f"Top 2 by price: {[p.name for p in products]}")
    
    # Count
    count = Product.query().count()
    print(f"Total products: {count}")
    
    print("✓ SQL Server Query Builder works!\n")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    """Run all SQL backend examples."""
    print("=" * 60)
    print("DBDuck Query Builder - All SQL Backends")
    print("=" * 60)
    print("Testing Query Builder with SQLite, MySQL, PostgreSQL, SQL Server\n")
    
    results = {
        "SQLite": run_sqlite_example(),
        "MySQL": run_mysql_example(),
        "PostgreSQL": run_postgresql_example(),
        "SQL Server": run_mssql_example(),
    }
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for backend, success in results.items():
        status = "✓ Passed" if success else "○ Skipped (not available)"
        print(f"  {backend}: {status}")
    
    print("\n" + "=" * 60)
    print("The Query Builder provides the same fluent API across all SQL backends:")
    print("  db.table('entity').where(...).order(...).limit(...).find()")
    print("  Model.query().where(...).find()  # Returns typed instances")
    print("=" * 60)


if __name__ == "__main__":
    main()
