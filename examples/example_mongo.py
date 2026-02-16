from udom.udom import UDOM

db = UDOM(db_type="nosql", db_instance="mongodb")

print(db.uquery("FIND User WHERE age > 25 AND active = true"))
print(db.uquery("DELETE User WHERE id = 5"))
print(db.uquery('CREATE User {name: "Veeresh", age: 23}'))
