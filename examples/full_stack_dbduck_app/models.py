"""DBDuck models used by the full stack showcase app."""

from __future__ import annotations

from DBDuck.models import (
    BooleanField,
    CASCADE,
    CharField,
    Column,
    DateTimeField,
    FloatField,
    ForeignKey,
    IntegerField,
    ManyToMany,
    ManyToOne,
    OneToMany,
    OneToOne,
    UModel,
)


class Customer(UModel):
    class Meta:
        db_table = "customers"

    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    email = Column(CharField, nullable=False, unique=True)
    password = Column(CharField, nullable=False)
    active = Column(BooleanField, default=True)
    role = Column(CharField, default="customer")


class Profile(UModel):
    class Meta:
        db_table = "profiles"

    id = Column(IntegerField, primary_key=True)
    customer_id = ForeignKey(Customer, on_delete=CASCADE)
    bio = Column(CharField, nullable=False, default="")


class Product(UModel):
    class Meta:
        db_table = "products"

    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False)
    price = Column(FloatField, nullable=False)
    active = Column(BooleanField, default=True)


class Order(UModel):
    class Meta:
        db_table = "orders"

    id = Column(IntegerField, primary_key=True)
    customer_id = ForeignKey(Customer, on_delete=CASCADE)
    paid = Column(BooleanField, default=False)
    status = Column(CharField, default="pending")
    created_at = Column(DateTimeField, default="")
    payment_provider = Column(CharField, default="")
    payment_order_id = Column(CharField, default="")
    payment_id = Column(CharField, default="")
    customer = ManyToOne(Customer, fk_field="customer_id")


class OrderItem(UModel):
    class Meta:
        db_table = "orderitems"

    id = Column(IntegerField, primary_key=True)
    order_id = ForeignKey(Order, on_delete=CASCADE)
    product_id = ForeignKey(Product, on_delete=CASCADE)
    quantity = Column(IntegerField, nullable=False, default=1)
    product = ManyToOne(Product, fk_field="product_id")


class Tag(UModel):
    class Meta:
        db_table = "tags"

    id = Column(IntegerField, primary_key=True)
    name = Column(CharField, nullable=False, unique=True)


class OrderTag(UModel):
    class Meta:
        db_table = "ordertags"

    id = Column(IntegerField, primary_key=True)
    order_id = Column(IntegerField, nullable=False)
    tag_id = Column(IntegerField, nullable=False)


Customer.profile = OneToOne(Profile, foreign_key="customer_id", local_key="id")
Customer.orders = OneToMany(Order, foreign_key="customer_id", local_key="id", order_by="id ASC")
Order.items = OneToMany(OrderItem, foreign_key="order_id", local_key="id", order_by="id ASC")
Order.tags = ManyToMany(Tag, through=OrderTag, from_key="order_id", to_key="tag_id")
Customer.__sensitive_fields__ = ["password"]


ALL_MODELS = [Customer, Profile, Product, Order, OrderItem, Tag, OrderTag]
