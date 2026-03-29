"""Identity helpers for the full stack DBDuck showcase app."""

from __future__ import annotations


def normalize_email(email: str) -> str:
    """Normalize email addresses for case-insensitive auth and uniqueness."""
    return email.strip().lower()
