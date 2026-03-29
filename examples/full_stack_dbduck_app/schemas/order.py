"""Order request schemas for the full stack DBDuck showcase app."""

from __future__ import annotations

from pydantic import BaseModel, Field


class OrderCreate(BaseModel):
    customer_id: int
    paid: bool = False
    status: str = Field(default="pending", min_length=1, max_length=40)
    items: list[dict[str, int]] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    