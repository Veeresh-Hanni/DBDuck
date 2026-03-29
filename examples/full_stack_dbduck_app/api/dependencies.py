"""API dependencies for the full stack DBDuck showcase app."""

from __future__ import annotations

from fastapi import Request

from DBDuck import UDOM
from DBDuck.core.exceptions import QueryError

from ..models import Customer
from ..services import customer_to_dict, decode_jwt


def get_db(request: Request) -> UDOM:
    return request.app.state.db


def get_current_user(request: Request) -> dict[str, object] | None:
    token = request.cookies.get("dbduck_access_token")
    if not token:
        return None
    try:
        payload = decode_jwt(token, secret=str(request.app.state.jwt_secret))
    except QueryError:
        return None
    customer_id = payload.get("sub")
    if not isinstance(customer_id, int):
        return None
    customer = Customer.query(get_db(request)).where(id=customer_id).first()
    return customer_to_dict(customer) if customer else None


def require_current_user(request: Request) -> dict[str, object]:
    user = get_current_user(request)
    if not user:
        raise QueryError("Login required")
    return user
