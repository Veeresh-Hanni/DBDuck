from udom import UDOM

db = UDOM(db_type="sql", db_instance="sqlite", url="sqlite:///udom.db")

print("Inserting data...")
db.uexecute('CREATE User {name: "Veeresh", age: 23, active: true}')
db.uexecute('CREATE User {name: "John", age: 30, active: false}')

print("\nFetching all users:")
print(db.uexecute("FIND User"))
