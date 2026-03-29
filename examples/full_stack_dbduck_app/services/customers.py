"""Customer service functions for the full stack DBDuck showcase app."""

from __future__ import annotations

from typing import Any

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError

from ..db import next_id
from ..models import Customer, Profile
from ..schemas import CustomerCreate
from .identity import normalize_email


def customer_to_dict(model: Customer) -> dict[str, Any]:
    profile = model.profile
    return {
        "id": model.id,
        "name": model.name,
        "email": model.email,
        "active": bool(model.active),
        "role": getattr(model, "role", "customer"),
        "bio": profile.bio if profile else "",
    }


def list_customers(db: UDOM) -> list[dict[str, Any]]:
    rows = Customer.query(db).order("id", "ASC").find()
    return [customer_to_dict(item) for item in rows]


def create_customer(db: UDOM, payload: CustomerCreate) -> dict[str, Any]:
    normalized_email = normalize_email(payload.email)
    existing = Customer.query(db).order("id", "ASC").find()
    if any(normalize_email(item.email) == normalized_email for item in existing):
        raise QueryError("An account with this email already exists")

    customer_id = next_id(Customer)
    profile_id = next_id(Profile)

    Customer(
        id=customer_id,
        name=payload.name,
        email=normalized_email,
        password=payload.password,
        active=True,
        role="customer",
    ).save(db=db)
    Profile(id=profile_id, customer_id=customer_id, bio=payload.bio or "").save(db=db)

    created = Customer.query(db).where(id=customer_id).first()
    return customer_to_dict(created) if created else {"id": customer_id}
