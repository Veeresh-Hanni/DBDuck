"""Service exports for the full stack DBDuck showcase app."""

from .auth import login_customer, require_admin, signup_customer
from .customers import create_customer, customer_to_dict, list_customers
from .dashboard import dashboard_stats, stats_payload
from .identity import normalize_email
from .jwt_tokens import create_jwt, decode_jwt
from .orders import create_order, joined_order_rows, list_orders, order_to_dict, prepare_order_items
from .payment_gateway import (
    create_payment_gateway_order,
    is_payment_gateway_configured,
    verify_payment_gateway_signature,
)
from .products import create_product, list_products
from .uql import uql_sample

__all__ = [
    "create_customer",
    "create_order",
    "create_product",
    "customer_to_dict",
    "dashboard_stats",
    "joined_order_rows",
    "login_customer",
    "list_customers",
    "list_orders",
    "list_products",
    "create_payment_gateway_order",
    "create_jwt",
    "is_payment_gateway_configured",
    "decode_jwt",
    "normalize_email",
    "order_to_dict",
    "prepare_order_items",
    "require_admin",
    "signup_customer",
    "stats_payload",
    "uql_sample",
    "verify_payment_gateway_signature",
]
