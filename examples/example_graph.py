from DBDuck import UDOM

db = UDOM(db_type="graph", db_instance="neo4j",
          url="bolt://localhost:7687",
          auth=("neo4j", "neo4j@123"))

# Create nodes
db.create("User",    {"id": "u1", "name": "Mira"})
db.create("Company", {"id": "c1", "name": "DBDuck"})

# Create relationship
db.create_relationship(
    "User", "u1", "WORKS_AT", "Company", "c1",
    {"role": "Engineer"})

# Find related nodes
companies = db.find_related(
    "User", id="u1",
    rel_type="WORKS_AT",
    target_label="Company")

# Shortest path
path = db.shortest_path(
    "User",
    "Company",
    from_id=1,
    to_id=2,
    max_depth=5
)