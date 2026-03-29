"""Authentication request schemas for the full stack DBDuck showcase app."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SignupRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    email: str = Field(..., min_length=3, max_length=120)
    password: str = Field(..., min_length=6, max_length=120)
    bio: str = Field(default="", max_length=200)


class LoginRequest(BaseModel):
    email: str = Field(..., min_length=3, max_length=120)
    password: str = Field(..., min_length=6, max_length=120)
