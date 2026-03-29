"""Product request schemas for the full stack DBDuck showcase app."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ProductCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    price: float = Field(..., gt=0)
