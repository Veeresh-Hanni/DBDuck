"""Input schema validation for UDOM operations."""

from __future__ import annotations

import re
from typing import Any, Mapping

from .exceptions import QueryError


class SchemaValidator:
    """Centralized runtime validation for UDOM operation payloads."""

    _entity_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    @classmethod
    def validate_entity(cls, entity: str) -> str:
        if not isinstance(entity, str) or not cls._entity_re.fullmatch(entity.strip()):
            raise QueryError("entity must be a valid identifier (letters, numbers, underscore)")
        return entity.strip()

    @classmethod
    def validate_create_data(cls, data: Mapping[str, Any]) -> Mapping[str, Any]:
        if not isinstance(data, Mapping) or not data:
            raise QueryError("data must be a non-empty mapping")
        for key in data:
            cls.validate_entity(str(key))
        return data

    @classmethod
    def validate_find_where(cls, where: Mapping[str, Any] | str | None) -> Mapping[str, Any] | str | None:
        if where is None:
            return None
        if isinstance(where, Mapping):
            if not where:
                raise QueryError("where mapping cannot be empty")
            for key, value in where.items():
                key_text = str(key)
                if key_text in {"$and", "$or"}:
                    if not isinstance(value, (list, tuple)) or not value:
                        raise QueryError(f"{key_text} must be a non-empty list of condition mappings")
                    for item in value:
                        if not isinstance(item, Mapping):
                            raise QueryError(f"{key_text} entries must be mappings")
                        cls.validate_find_where(item)
                    continue
                field_name = key_text.rsplit("__", 1)[0] if "__" in key_text else key_text
                cls.validate_entity(field_name)
            return where
        if isinstance(where, str):
            text = where.strip()
            if not text:
                raise QueryError("where string cannot be empty")
            return text
        raise QueryError("where must be a mapping, string, or None")
