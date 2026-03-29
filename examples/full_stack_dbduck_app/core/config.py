"""Configuration helpers for the full stack DBDuck showcase app."""

from __future__ import annotations

import os


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def get_jwt_secret() -> str:
    return get_env("APP_JWT_SECRET", "dbduck-demo-change-me")


def get_payment_gateway_key_id() -> str:
    return get_env("APP_PAYMENT_GATEWAY_KEY_ID", get_env("APP_RAZORPAY_KEY_ID", ""))


def get_payment_gateway_key_secret() -> str:
    return get_env("APP_PAYMENT_GATEWAY_KEY_SECRET", get_env("APP_RAZORPAY_KEY_SECRET", ""))
