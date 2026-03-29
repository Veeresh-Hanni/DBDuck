"""Order service functions for the full stack DBDuck showcase app."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError

from ..db import next_id
from ..models import Customer, Order, OrderItem, OrderTag, Product, Tag
from ..schemas import OrderCreate


def order_to_dict(model: Order) -> dict[str, Any]:
    customer = model.customer
    items = []
    total = 0.0
    for item in model.items:
        product = item.product
        line_total = float(product.price) * int(item.quantity) if product else 0.0
        total += line_total
        items.append(
            {
                "id": item.id,
                "product_id": item.product_id,
                "product_name": product.name if product else None,
                "quantity": item.quantity,
                "line_total": round(line_total, 2),
            }
        )
    return {
        "id": model.id,
        "customer_id": model.customer_id,
        "customer_name": customer.name if customer else None,
        "paid": bool(model.paid),
        "status": model.status,
        "created_at": model.created_at,
        "items": items,
        "tags": [tag.name for tag in model.tags],
        "total": round(total, 2),
        "payment_provider": getattr(model, "payment_provider", ""),
        "payment_order_id": getattr(model, "payment_order_id", ""),
        "payment_id": getattr(model, "payment_id", ""),
    }


def joined_order_rows(db: UDOM, *, customer_id: int | None = None) -> list[dict[str, Any]]:
    query = (
        db.table("orders")
        .join("customers", on=("customer_id", "id"))
        .select("id", "customer_id", "status", "paid", "customers.name")
        .order("id", "ASC")
    )
    if customer_id is not None:
        query = query.where(customer_id=customer_id)
    return query.find()


def list_orders(db: UDOM, *, customer_id: int | None = None) -> list[dict[str, Any]]:
    query = Order.query(db).order("id", "ASC")
    if customer_id is not None:
        query = query.where(customer_id=customer_id)
    page = query.find_page(page=1, page_size=20)
    return [order_to_dict(item) for item in page["items"]]


def prepare_order_items(db: UDOM, payload_items: list[dict[str, int]]) -> tuple[list[dict[str, int]], float]:
    if not payload_items:
        raise QueryError("Order must include at least one item")

    normalized_items: list[dict[str, int]] = []
    total = 0.0
    for index, item in enumerate(payload_items, start=1):
        product_id = item.get("product_id")
        quantity = int(item.get("quantity", 1))
        if not isinstance(product_id, int):
            raise QueryError(f"Order item {index} must include an integer product_id")
        if quantity <= 0:
            raise QueryError(f"Order item {index} quantity must be greater than zero")
        product = Product.query(db).where(id=product_id).first()
        if product is None:
            raise QueryError(f"Product {product_id} does not exist")
        normalized_items.append({"product_id": product_id, "quantity": quantity})
        total += float(product.price) * quantity
    return normalized_items, round(total, 2)


def create_order(
    db: UDOM,
    payload: OrderCreate,
    *,
    payment_provider: str = "",
    payment_order_id: str = "",
    payment_id: str = "",
) -> dict[str, Any]:
    customer = Customer.query(db).where(id=payload.customer_id).first()
    if customer is None:
        raise QueryError(f"Customer {payload.customer_id} does not exist")
    normalized_items, _total = prepare_order_items(db, payload.items)

    order_id = next_id(Order)
    created_at = datetime.utcnow().isoformat()

    with db.transaction():
        Order(
            id=order_id,
            customer_id=payload.customer_id,
            paid=payload.paid,
            status=payload.status,
            created_at=created_at,
            payment_provider=payment_provider,
            payment_order_id=payment_order_id,
            payment_id=payment_id,
        ).save(db=db)

        next_item_id = next_id(OrderItem)
        for index, item in enumerate(normalized_items):
            OrderItem(
                id=next_item_id + index,
                order_id=order_id,
                product_id=item["product_id"],
                quantity=item["quantity"],
            ).save(db=db)

        next_tag_id = next_id(Tag)
        next_link_id = next_id(OrderTag)
        for index, tag_name in enumerate(payload.tags):
            existing_tag = Tag.query(db).where(name=tag_name).first()
            tag = existing_tag or Tag(id=next_tag_id + index, name=tag_name)
            if existing_tag is None:
                tag.save(db=db)
            OrderTag(id=next_link_id + index, order_id=order_id, tag_id=tag.id).save(db=db)

    created = Order.query(db).where(id=order_id).first()
    return order_to_dict(created) if created else {"id": order_id}
