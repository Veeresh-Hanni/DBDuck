"""SQLite adapter."""

from __future__ import annotations

from typing import Any

from ._sqlalchemy_adapter import SQLAlchemyAdapter


class SQLiteAdapter(SQLAlchemyAdapter):
    DIALECT = "sqlite"

    def _quote(self, name: str) -> str:
        return f'"{name}"'

    def _pk_column_sql(self) -> str:
        return '"id" INTEGER PRIMARY KEY AUTOINCREMENT'

    def _type_for_value(self, value: Any) -> str:
        if isinstance(value, bool):
            return "INTEGER"
        if isinstance(value, int):
            return "INTEGER"
        if isinstance(value, float):
            return "REAL"
        return "TEXT"
