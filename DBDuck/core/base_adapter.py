"""Adapter interface and shared SQL adapter utilities."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping


class BaseAdapter(ABC):
    """Common adapter contract for UDOM routing."""

    @abstractmethod
    def run_native(self, query: str, params: Mapping[str, Any] | None = None) -> Any:
        """Execute a native backend query."""

    @abstractmethod
    def convert_uql(self, uql_query: str) -> Any:
        """Convert UQL to backend-native query language."""

    @abstractmethod
    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        """Create a new record/document for the given entity."""

    @abstractmethod
    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        """Batch-create records/documents."""

    @abstractmethod
    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        """Find records/documents from an entity."""

    @abstractmethod
    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        """Delete records/documents from an entity."""
