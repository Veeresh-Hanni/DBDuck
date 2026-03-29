"""Dashboard service functions for the full stack DBDuck showcase app."""

from __future__ import annotations

from typing import Any

from DBDuck import UDOM

from ..models import Customer, Order, Product
from .orders import joined_order_rows


def dashboard_stats(db: UDOM) -> dict[str, Any]:
    return {
        "customers": Customer.query(db).count(),
        "products": Product.query(db).count(),
        "orders": Order.query(db).count(),
        "completed_orders": Order.query(db).where(status="completed").count(),
    }


def stats_payload(db: UDOM) -> dict[str, Any]:
    return {
        "aggregate": (
            db.table("orders")
            .group_by("paid")
            .order_by("paid DESC")
            .aggregate(metrics={"total_orders": "count(*)"})
        ),
        "joined_orders": joined_order_rows(db),
    }
