from udom import UDOM

# New style: db_type + db_instance

db = UDOM(db_type="sql", db_instance="mysql", url="mysql+pymysql://root:Veeru123@localhost:3306/udom")

print("Native SQL Query (db.query)")
print(db.query("SELECT * FROM `User`;"))

print("\nInserting data using UQL")
db.uexecute('CREATE User {name: "Veeresh", age: 23, active: true}')
db.uexecute('CREATE User {name: "John", age: 30, active: false}')

print("\nFetching all users using UQL")
print(db.uexecute("FIND User"))

print("\nFetching users age > 21 using UQL")
print(db.uexecute("FIND User WHERE age > 21"))
