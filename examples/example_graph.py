from udom.udom import UDOM

db = UDOM(db_type="graph", db_instance="neo4j")

print(db.uquery("FIND User WHERE age > 25 AND active = true"))
print(db.uquery('CREATE User {name: "Veeresh", age: 23}'))
print(db.uquery("FIND User WHERE HAS friends AND age > 25"))
print(db.uquery("DELETE User WHERE active = false"))
