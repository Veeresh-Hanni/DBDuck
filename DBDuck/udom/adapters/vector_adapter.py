from typing import Any, Mapping

from ...core.exceptions import QueryError
from .base_adapter import BaseAdapter


class VectorAdapter(BaseAdapter):
    """Adapter stub for vector databases (Qdrant, Pinecone, Weaviate, etc.)."""

    def __init__(self, db_instance="qdrant", url=None, **options):
        self.db_instance = db_instance
        self.url = url
        self.options = options

    def run_native(self, query: Any, params: Mapping[str, Any] | None = None):
        if params:
            raise QueryError("VectorAdapter does not support params argument")
        return {
            "db_type": "vector",
            "db_instance": self.db_instance,
            "url": self.url,
            "native_query": query,
            "note": "Vector adapter is currently a pass-through stub.",
        }

    def convert_uql(self, uql_query: str):
        return {
            "action": "vector_uql_passthrough",
            "db_instance": self.db_instance,
            "uql": uql_query,
        }

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        return self.run_native({"action": "create", "entity": entity, "data": dict(data)})

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        return self.run_native({"action": "create_many", "entity": entity, "rows": [dict(r) for r in rows]})

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self.run_native(
            {"action": "find", "entity": entity, "where": where, "order_by": order_by, "limit": limit}
        )

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        return self.run_native({"action": "delete", "entity": entity, "where": where})
