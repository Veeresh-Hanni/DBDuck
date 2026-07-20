from __future__ import annotations

import urllib

from DBDuck import UDOM
from DBDuck.models import (
    BooleanField,
    CharField,
    Column,
    ForeignKey,
    IntegerField,
    ManyToMany,
    ManyToOne,
    OneToMany,
    OneToOne,
    UModel,
    CASCADE
)


class Customer(UModel):
    class Meta:
        db_table = "customers_django_style"

    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)


class Order(UModel):
    class Meta:
        db_table = "orders_django_style"

    id = Column(IntegerField, primary_key=True)
    customer_id = ForeignKey(Customer, on_delete=CASCADE)
    paid = Column(BooleanField, default=False)
    customer = ManyToOne(Customer, fk_field="customer_id")


class Profile(UModel):
    class Meta:
        db_table = "profiles_django_style"

    id = Column(IntegerField, primary_key=True)
    customer_id = ForeignKey(Customer, on_delete=CASCADE)
    bio = Column(CharField, nullable=False)


class Tag(UModel):
    class Meta:
        db_table = "tags_django_style"

    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)


class OrderTag(UModel):
    class Meta:
        db_table = "order_tags_django_style"

    id = Column(IntegerField, primary_key=True)
    order_id = Column(IntegerField, nullable=False)
    tag_id = Column(IntegerField, nullable=False)


Customer.profile = OneToOne(Profile, foreign_key="customer_id", local_key="id")
Customer.orders = OneToMany(Order, foreign_key="customer_id", local_key="id")
Order.tags = ManyToMany(Tag, through=OrderTag, from_key="order_id", to_key="tag_id")

# mysql
# db_user = "user_name"
# db_pass = "pass" # Password-nalli symbols idre quote_plus use madi
# db_hostname = "***.rds.amazonaws.com"
db_name = "customer_db"
# port = 3306
# base_url = f"mysql+pymysql://{db_user}:{urllib.parse.quote_plus(db_pass)}@{db_hostname}:{port}/{db_name}"

# psql
url = f'postgresql://username:pass@loclahost:6543/{db_name}?sslmode=require'
# 1. Database name illade URL build madi
# Note: port aamele '/' matra ide, db name illa.

def main() -> None:
    db = UDOM(db_type="sql", db_instance="postgres", url=url)
    Customer.bind(db)
    Order.bind(db)
    Profile.bind(db)
    Tag.bind(db)
    OrderTag.bind(db)

    print(Customer(id=1, name="Alice").save())
    alice = Customer.find_one(where={"id": 1})
    print(Order(id=101, customer_id=alice, paid=False).save())
    print(Profile(id=11, customer_id=1, bio="VIP customer").save())
    print(Tag(id=1, name="urgent").save())
    print(Tag(id=2, name="paid").save())
    print(OrderTag(id=1001, order_id=101, tag_id=1).save())
    print(OrderTag(id=1002, order_id=101, tag_id=2).save())

    print("many_to_one:", Order.find_one(where={"id": 101}).customer.to_dict())
    print("one_to_one:", Customer.find_one(where={"id": 1}).profile.to_dict())
    print("one_to_many:", [o.to_dict() for o in Customer.find_one(where={"id": 1}).orders])
    print("many_to_many:", [t.to_dict() for t in Order.find_one(where={"id": 101}).tags])

    print([m.to_dict() for m in Order.find(where={"paid": False})])
    print(Order.find_one(where={"id": 101}).update(data={"paid": True}, where={"id": 101}))
    page = Order.find_page(page=1, page_size=10, order_by="id ASC")
    page["items"] = [m.to_dict() for m in page["items"]]
    print(page)
    print(Order.find_one(where={"id": 101}).delete(where={"id": 101}))
    print(Customer.find_one(where={"id": 1}).delete(where={"id": 1}))


if __name__ == "__main__":
    main()
