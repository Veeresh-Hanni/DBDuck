"""PostgreSQL adapter."""

from __future__ import annotations

from typing import Any

from ._sqlalchemy_adapter import SQLAlchemyAdapter


class PostgresAdapter(SQLAlchemyAdapter):
    DIALECT = "postgres"

    def _quote(self, name: str) -> str:
        return f'"{name}"'

    def _pk_column_sql(self) -> str:
        return '"id" SERIAL PRIMARY KEY'

    def _type_for_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return "BOOLEAN"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "DOUBLE PRECISION"
        return "TEXT"
