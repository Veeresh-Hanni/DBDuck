"""Razorpay integration helpers for the full stack DBDuck showcase app."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from DBDuck.core.exceptions import QueryError


def is_razorpay_configured(*, key_id: str, key_secret: str) -> bool:
    return bool(key_id and key_secret)


def create_razorpay_order(
    *,
    amount_rupees: float,
    receipt: str,
    key_id: str,
    key_secret: str,
    notes: dict[str, str] | None = None,
) -> dict[str, Any]:
    if not is_razorpay_configured(key_id=key_id, key_secret=key_secret):
        raise QueryError("Razorpay is not configured on this server")

    payload = json.dumps(
        {
            "amount": int(round(amount_rupees * 100)),
            "currency": "INR",
            "receipt": receipt,
            "payment_capture": 1,
            "notes": notes or {},
        }
    ).encode("utf-8")
    auth = base64.b64encode(f"{key_id}:{key_secret}".encode("utf-8")).decode("ascii")
    request = Request(
        "https://api.razorpay.com/v1/orders",
        data=payload,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="ignore")
        raise QueryError(f"Razorpay order creation failed: {details or exc.reason}") from exc
    except URLError as exc:
        raise QueryError("Unable to reach Razorpay") from exc


def verify_razorpay_signature(
    *,
    order_id: str,
    payment_id: str,
    signature: str,
    key_secret: str,
) -> None:
    if not is_razorpay_configured(key_id="configured", key_secret=key_secret):
        raise QueryError("Razorpay is not configured on this server")
    payload = f"{order_id}|{payment_id}".encode("utf-8")
    digest = hmac.new(key_secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(digest, signature):
        raise QueryError("Invalid Razorpay payment signature")
