"""Authentication service functions for the full stack DBDuck showcase app."""

from __future__ import annotations

from typing import Any

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError

from ..models import Customer
from ..schemas import LoginRequest, SignupRequest
from .customers import create_customer, customer_to_dict
from .identity import normalize_email


def signup_customer(db: UDOM, payload: SignupRequest) -> dict[str, Any]:
    return create_customer(db, payload)


def login_customer(db: UDOM, payload: LoginRequest) -> dict[str, Any]:
    normalized_email = normalize_email(payload.email).lower()
    customer = next(
        (item for item in Customer.query(db).order("id", "ASC").find() if normalize_email(item.email) == normalized_email),
        None,
    )
    if customer is None:
        raise QueryError("No account found for this email")
    if not customer.verify_secret("password", payload.password):
        raise QueryError("Invalid email or password")
    return customer_to_dict(customer)


def require_admin(user: dict[str, Any] | None) -> None:
    if not user:
        raise QueryError("Login required")
    if user.get("role") != "admin":
        raise QueryError("Admin access required")
