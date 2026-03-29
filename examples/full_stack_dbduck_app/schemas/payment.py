"""Payment request schemas for the full stack DBDuck showcase app."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RazorpayCheckoutRequest(BaseModel):
    items: list[dict[str, int]] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    status: str = Field(default="completed", min_length=1, max_length=40)


class RazorpayCompleteRequest(BaseModel):
    razorpay_order_id: str = Field(..., min_length=1, max_length=120)
    razorpay_payment_id: str = Field(..., min_length=1, max_length=120)
    razorpay_signature: str = Field(..., min_length=1, max_length=256)
    items: list[dict[str, int]] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    status: str = Field(default="completed", min_length=1, max_length=40)
