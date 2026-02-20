"""MySQL adapter."""

from __future__ import annotations

from typing import Any

from ._sqlalchemy_adapter import SQLAlchemyAdapter


class MySQLAdapter(SQLAlchemyAdapter):
    DIALECT = "mysql"

    def _quote(self, name: str) -> str:
        return f"`{name}`"

    def _pk_column_sql(self) -> str:
        return "`id` INT PRIMARY KEY AUTO_INCREMENT"

    def _type_for_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "INT"
        if isinstance(value, float):
            return "DOUBLE"
        return "VARCHAR(255)"
