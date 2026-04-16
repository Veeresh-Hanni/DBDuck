import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from DBDuck import UDOM, BooleanField, CharField, IntegerField, UModel, models


class User(UModel):
    # class Meta:
    #     db_table = "users"
    __entity__ = "users"
    id = models.Column(IntegerField, primary_key=True)
    name = models.Column(CharField, nullable=False)
    role = models.Column(CharField, default="user")
    active = models.Column(models.BooleanField, default=False)
    age = models.Column(IntegerField, nullable=True)
    paltu=models.Column(BooleanField, nullable=True)


db = UDOM(url="sqlite:///app.db")
User.bind(db)

# Create or migrate schema before reads.
print(User.migrate(db))

# Seed once.
if User.query().where(id=1).first() is None:
    User(id=1, name="Alice", role="admin", active=True, age=25).save()

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

print(User.migration_history())
