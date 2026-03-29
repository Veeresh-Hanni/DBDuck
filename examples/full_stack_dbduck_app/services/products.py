"""Product service functions for the full stack DBDuck showcase app."""

from __future__ import annotations

from DBDuck import UDOM

from ..db import next_id
from ..models import Product
from ..schemas import ProductCreate


def list_products(db: UDOM) -> list[dict[str, object]]:
    return [item.to_dict() for item in Product.query(db).order("id", "ASC").find()]


def create_product(db: UDOM, payload: ProductCreate) -> dict[str, object]:
    product_id = next_id(Product)
    product = Product(id=product_id, name=payload.name, price=payload.price, active=True)
    product.save(db=db)
    return product.to_dict()
