"""UQL validation helpers."""

from __future__ import annotations

import re
from typing import TypedDict


class ValidationResult(TypedDict, total=False):
    valid: bool
    error: str
    message: str


class UQLValidator:
    """Validates UQL syntax and basic injection patterns."""

    allowed_actions = ("FIND", "CREATE", "UPDATE", "DELETE")
    _dangerous = re.compile(r"(?:--|/\*|\*/|;\s*(?:DROP|TRUNCATE|ALTER|CREATE)\b)", re.IGNORECASE)

    def is_valid_syntax(self, uql_query: str) -> bool:
        if not isinstance(uql_query, str):
            return False
        stripped = uql_query.strip().upper()
        return any(stripped.startswith(action) for action in self.allowed_actions)

    def check_for_injection(self, query: str) -> bool:
        if not isinstance(query, str):
            return False
        return self._dangerous.search(query) is None

    def validate(self, uql_query: str) -> ValidationResult:
        if not self.is_valid_syntax(uql_query):
            return {"valid": False, "error": "Invalid UQL syntax"}
        if not self.check_for_injection(uql_query):
            return {"valid": False, "error": "Potential injection risk"}
        return {"valid": True, "message": "UQL is valid"}
