"""
Query Builder Across All Database Backends

Demonstrates the Query Builder DSL working uniformly across:
- SQL (SQLite)
- NoSQL (MongoDB) 
- Graph (Neo4j)
- Vector (Qdrant)

Note: This example requires running instances of MongoDB, Neo4j, and Qdrant.
If not available, those sections will be skipped gracefully.
"""

from DBDuck import UDOM


def run_sql_examples():
    """Query Builder with SQL (SQLite - always available)."""
    print("\n" + "=" * 60)
    print("SQL (SQLite) - Query Builder")
    print("=" * 60)
    
    db = UDOM(url="sqlite:///:memory:")
    
    # Create table
    db.adapter.run_native("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT,
            price REAL,
            in_stock INTEGER
        )
    """)
    
    # Insert data using Query Builder
    db.table("products").create({"id": 1, "name": "Laptop", "category": "electronics", "price": 999.99, "in_stock": 1})
    db.table("products").create({"id": 2, "name": "Mouse", "category": "electronics", "price": 29.99, "in_stock": 1})
    db.table("products").create({"id": 3, "name": "Desk", "category": "furniture", "price": 249.99, "in_stock": 1})
    db.table("products").create({"id": 4, "name": "Chair", "category": "furniture", "price": 149.99, "in_stock": 0})
    
    # Query Builder operations
    all_products = db.table("products").find()
    print(f"All products: {len(all_products)}")
    
    electronics = db.table("products").where(category="electronics").find()
    print(f"Electronics: {[p['name'] for p in electronics]}")
    
    in_stock = db.table("products").where(in_stock=1).order("price", "DESC").find()
    print("In stock (by price): " + str([f"{p['name']} (${p['price']})" for p in in_stock]))
    
    expensive = db.table("products").where_gt(price=100).count()
    print(f"Products over $100: {expensive}")
    
    print("✓ SQL Query Builder works!\n")


def run_mongodb_examples():
    """Query Builder with MongoDB."""
    print("\n" + "=" * 60)
    print("NoSQL (MongoDB) - Query Builder")
    print("=" * 60)
    
    try:
        db = UDOM(db_type="nosql", db_instance="mongodb", url="mongodb://localhost:27017/dbduck_test")
        db.ping()
    except Exception as e:
        print(f"MongoDB not available: {e}")
        print("Skipping MongoDB examples (start MongoDB to test)\n")
        return
    
    # Clean up and insert data
    try:
        db.delete("profiles", where={})  # Clear collection
    except:
        pass
    
    db.table("profiles").create({"name": "Alice", "department": "Engineering", "active": True})
    db.table("profiles").create({"name": "Bob", "department": "Sales", "active": True})
    db.table("profiles").create({"name": "Charlie", "department": "Engineering", "active": False})
    
    # Query Builder operations
    all_profiles = db.table("profiles").find()
    print(f"All profiles: {len(all_profiles)}")
    
    engineering = db.table("profiles").where(department="Engineering").find()
    print(f"Engineering: {[p['name'] for p in engineering]}")
    
    active = db.table("profiles").where(active=True).find()
    print(f"Active profiles: {[p['name'] for p in active]}")
    
    count = db.table("profiles").where(department="Engineering").count()
    print(f"Engineering count: {count}")
    
    print("✓ MongoDB Query Builder works!\n")


def run_neo4j_examples():
    """Query Builder with Neo4j."""
    print("\n" + "=" * 60)
    print("Graph (Neo4j) - Query Builder")
    print("=" * 60)
    
    try:
        db = UDOM(
            db_type="graph", 
            db_instance="neo4j", 
            url="bolt://localhost:7687",
            auth=("neo4j", "password")
        )
        db.ping()
    except Exception as e:
        print(f"Neo4j not available: {e}")
        print("Skipping Neo4j examples (start Neo4j to test)\n")
        return
    
    # Clean up
    try:
        db.delete("Person", where={"name": "Alice"})
        db.delete("Person", where={"name": "Bob"})
        db.delete("Company", where={"name": "TechCorp"})
    except:
        pass
    
    # Create nodes using Query Builder
    db.table("Person").create({"id": "p1", "name": "Alice", "role": "Engineer"})
    db.table("Person").create({"id": "p2", "name": "Bob", "role": "Manager"})
    db.table("Company").create({"id": "c1", "name": "TechCorp"})
    
    # Find nodes
    people = db.table("Person").find()
    print(f"People: {len(people)}")
    
    alice = db.table("Person").where(name="Alice").first()
    print(f"Found Alice: {alice}")
    
    # Create relationship
    db.table("Person").create_relationship("p1", "WORKS_AT", "Company", "c1", {"since": "2020"})
    
    # Find related nodes
    alice_company = db.table("Person").find_related(id="p1", rel_type="WORKS_AT")
    print(f"Alice works at: {alice_company}")
    
    print("✓ Neo4j Query Builder works!\n")


def run_qdrant_examples():
    """Query Builder with Qdrant."""
    print("\n" + "=" * 60)
    print("Vector (Qdrant) - Query Builder")
    print("=" * 60)
    
    try:
        db = UDOM(db_type="vector", db_instance="qdrant", url="http://localhost:6333")
        db.ping()
    except Exception as e:
        print(f"Qdrant not available: {e}")
        print("Skipping Qdrant examples (start Qdrant to test)\n")
        return
    
    # Create collection
    try:
        db.create_collection("products", vector_size=3, distance="cosine")
    except:
        pass  # Collection might exist
    
    # Insert vectors using Query Builder
    db.table("products").upsert_vector("v1", [0.1, 0.2, 0.3], {"name": "Laptop", "category": "electronics"})
    db.table("products").upsert_vector("v2", [0.2, 0.3, 0.4], {"name": "Phone", "category": "electronics"})
    db.table("products").upsert_vector("v3", [0.9, 0.8, 0.7], {"name": "Desk", "category": "furniture"})
    
    # Search similar vectors
    similar = db.table("products").search_similar([0.1, 0.2, 0.3], top_k=2)
    print(f"Similar to [0.1, 0.2, 0.3]:")
    for item in similar:
        print(f"  - {item['metadata'].get('name')} (score: {item['score']:.3f})")
    
    # Find with filter
    electronics = db.table("products").where(category="electronics").find()
    print(f"Electronics: {len(electronics)}")
    
    # Count
    count = db.table("products").count()
    print(f"Total products: {count}")
    
    print("✓ Qdrant Query Builder works!\n")


def main():
    """Run all examples."""
    print("=" * 60)
    print("DBDuck Query Builder - All Backends Demo")
    print("=" * 60)
    print("This demonstrates the same Query Builder API working")
    print("across SQL, NoSQL, Graph, and Vector databases.\n")
    
    # SQL always works (SQLite in-memory)
    run_sql_examples()
    
    # These require running services
    run_mongodb_examples()
    run_neo4j_examples()
    run_qdrant_examples()
    
    print("=" * 60)
    print("Query Builder Demo Complete!")
    print("=" * 60)
    print("\nThe Query Builder provides a unified, fluent API across all backends:")
    print("  - db.table('entity').where(...).order(...).limit(...).find()")
    print("  - Same pattern for SQL, MongoDB, Neo4j, and Qdrant")
    print("  - Backend-specific methods like search_similar() for vectors")


if __name__ == "__main__":
    main()
