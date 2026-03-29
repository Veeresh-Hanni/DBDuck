"""API routes for the full stack DBDuck showcase app."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response

from DBDuck import UDOM

from ..schemas import (
    LoginRequest,
    OrderCreate,
    RazorpayCheckoutRequest,
    RazorpayCompleteRequest,
    ProductCreate,
    SignupRequest,
)
from ..services import (
    create_order,
    create_payment_gateway_order,
    create_product,
    dashboard_stats,
    is_payment_gateway_configured,
    joined_order_rows,
    login_customer,
    list_orders,
    list_products,
    create_jwt,
    prepare_order_items,
    require_admin,
    signup_customer,
    stats_payload,
    uql_sample,
    verify_payment_gateway_signature,
)
from .dependencies import get_current_user, get_db, require_current_user


router = APIRouter(prefix="/api", tags=["showcase"])


def _set_session_cookie(request: Request, response: Response, user_id: int) -> None:
    ttl_seconds = int(getattr(request.app.state, "session_ttl_seconds", 3600))
    token = create_jwt(
        subject=int(user_id),
        secret=str(request.app.state.jwt_secret),
        ttl_seconds=ttl_seconds,
    )
    response.set_cookie(
        "dbduck_access_token",
        token,
        httponly=True,
        samesite="lax",
        max_age=ttl_seconds,
    )


@router.get("/health")
def health(db: UDOM = Depends(get_db)):
    return {"status": "ok", "ping": db.ping()}


@router.get("/dashboard")
def dashboard(db: UDOM = Depends(get_db)):
    return dashboard_stats(db)


@router.get("/auth/me")
def auth_me(user: dict[str, object] | None = Depends(get_current_user)):
    return {"user": user}


@router.post("/auth/signup")
def auth_signup(
    payload: SignupRequest,
    request: Request,
    response: Response,
    db: UDOM = Depends(get_db),
):
    user = signup_customer(db, payload)
    _set_session_cookie(request, response, int(user["id"]))
    return {"user": user}


@router.post("/auth/login")
def auth_login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    db: UDOM = Depends(get_db),
):
    user = login_customer(db, payload)
    _set_session_cookie(request, response, int(user["id"]))
    return {"user": user}


@router.post("/auth/logout")
def auth_logout(_request: Request, response: Response):
    response.delete_cookie("dbduck_access_token")
    return {"ok": True}


@router.get("/products")
def products(db: UDOM = Depends(get_db)):
    return list_products(db)


@router.post("/products")
def products_create(
    payload: ProductCreate,
    db: UDOM = Depends(get_db),
    user: dict[str, object] = Depends(require_current_user),
):
    require_admin(user)
    return create_product(db, payload)


@router.get("/orders")
def orders(
    db: UDOM = Depends(get_db),
    user: dict[str, object] = Depends(require_current_user),
):
    if user.get("role") == "admin":
        return list_orders(db)
    return list_orders(db, customer_id=int(user["id"]))


@router.get("/orders/joined")
def orders_joined(
    db: UDOM = Depends(get_db),
    user: dict[str, object] = Depends(require_current_user),
):
    if user.get("role") == "admin":
        return joined_order_rows(db)
    return joined_order_rows(db, customer_id=int(user["id"]))


@router.post("/orders")
def orders_create(
    payload: OrderCreate,
    db: UDOM = Depends(get_db),
    user: dict[str, object] = Depends(require_current_user),
):
    if int(user["id"]) != payload.customer_id and user.get("role") != "admin":
        from DBDuck.core.exceptions import QueryError

        raise QueryError("You can only place orders for your own account")
    return create_order(db, payload)


@router.get("/payments/config")
def payment_gateway_config(request: Request):
    configured = is_payment_gateway_configured(
        key_id=str(request.app.state.payment_gateway_key_id),
        key_secret=str(request.app.state.payment_gateway_key_secret),
    )
    return {
        "configured": configured,
        "key_id": str(request.app.state.payment_gateway_key_id) if configured else "",
    }


@router.post("/payments/checkout")
def payment_gateway_checkout(
    payload: RazorpayCheckoutRequest,
    request: Request,
    db: UDOM = Depends(get_db),
    user: dict[str, object] = Depends(require_current_user),
):
    normalized_items, total = prepare_order_items(db, payload.items)
    gateway_order = create_payment_gateway_order(
        amount_rupees=total,
        receipt=f"dbduck-{user['id']}-{len(normalized_items)}",
        key_id=str(request.app.state.payment_gateway_key_id),
        key_secret=str(request.app.state.payment_gateway_key_secret),
        notes={
            "customer_id": str(user["id"]),
            "customer_email": str(user["email"]),
        },
    )
    return {
        "key": str(request.app.state.payment_gateway_key_id),
        "order_id": gateway_order["id"],
        "amount": gateway_order["amount"],
        "currency": gateway_order.get("currency", "INR"),
        "customer": {
            "name": user["name"],
            "email": user["email"],
        },
        "normalized_items": normalized_items,
        "total": total,
    }


@router.post("/payments/complete")
def payment_gateway_complete(
    payload: RazorpayCompleteRequest,
    request: Request,
    db: UDOM = Depends(get_db),
    user: dict[str, object] = Depends(require_current_user),
):
    verify_payment_gateway_signature(
        order_id=payload.razorpay_order_id,
        payment_id=payload.razorpay_payment_id,
        signature=payload.razorpay_signature,
        key_secret=str(request.app.state.payment_gateway_key_secret),
    )
    local_order = create_order(
        db,
        OrderCreate(
            customer_id=int(user["id"]),
            paid=True,
            status=payload.status,
            items=payload.items,
            tags=payload.tags,
        ),
        payment_provider="payment_gateway",
        payment_order_id=payload.razorpay_order_id,
        payment_id=payload.razorpay_payment_id,
    )
    return {"order": local_order}


@router.get("/stats")
def stats(db: UDOM = Depends(get_db)):
    return stats_payload(db)


@router.get("/uql/sample")
def sample_uql(db: UDOM = Depends(get_db)):
    return uql_sample(db)
