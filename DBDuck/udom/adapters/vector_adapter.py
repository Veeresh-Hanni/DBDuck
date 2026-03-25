"""Vector database adapter with live Qdrant support and documented stubs for others."""

from __future__ import annotations

from typing import Any, Mapping
from uuid import uuid4

from ...core.exceptions import ConnectionError, QueryError
from ...utils.logger import get_logger, log_event, log_internal_debug
from .base_adapter import BaseAdapter


class VectorAdapter(BaseAdapter):
    """Adapter for vector databases, with Qdrant as the first production backend."""

    def __init__(self, db_instance: str = "qdrant", url: str | None = None, **options: Any) -> None:
        self.db_instance = db_instance.lower().strip()
        self.url = url
        self.options = options
        self._logger = get_logger(options.get("log_level"))
        self._client = options.get("client")
        self._models = None
        self._distance_map = None

    def _get_qdrant(self):
        if self.db_instance != "qdrant":
            raise QueryError(
                f"{self.db_instance} support is currently a documented stub"
            )

        if self._client is not None:
            return self._client

        try:
            from qdrant_client import QdrantClient
            from qdrant_client.http import models as rest
            from urllib.parse import urlparse
        except Exception as exc:
            raise ConnectionError(
                "qdrant-client is required for Qdrant support"
            ) from exc

        self._models = rest

        self._distance_map = {
            "cosine": rest.Distance.COSINE,
            "euclid": rest.Distance.EUCLID,
            "dot": rest.Distance.DOT,
        }

        url = self.url or "http://localhost:6333"

        try:
            # ✅ Try new API (1.x)
            self._client = QdrantClient(url=url)

        except TypeError:

            # ✅ fallback for old API (0.x)
            try:
                parsed = urlparse(url)

                host = parsed.hostname or "localhost"
                port = parsed.port or 6333

                self._client = QdrantClient(
                    host=host,
                    port=port,
                )

            except Exception as exc:
                raise ConnectionError(
                    "Database connection failed"
                ) from exc

        except Exception as exc:
            raise ConnectionError(
                "Database connection failed"
            ) from exc

        return self._client

    def _ensure_models_loaded(self) -> None:
        if self._models is not None and self._distance_map is not None:
            return
        if self._client is not None and self._models is not None and self._distance_map is not None:
            return
        try:
            from qdrant_client.http import models as rest
        except Exception:
            return
        self._models = rest
        self._distance_map = {
            "cosine": rest.Distance.COSINE,
            "euclid": rest.Distance.EUCLID,
            "dot": rest.Distance.DOT,
        }

    @staticmethod
    def _validate_entity(entity: str) -> str:
        if not isinstance(entity, str) or not entity.strip():
            raise QueryError("entity must be a non-empty string")
        return entity.strip()

    @staticmethod
    def _validate_vector(vector: Any) -> list[float]:
        if not isinstance(vector, (list, tuple)) or not vector:
            raise QueryError("vector must be a non-empty list or tuple")
        values: list[float] = []
        for item in vector:
            if not isinstance(item, (int, float)):
                raise QueryError("vector values must be numeric")
            values.append(float(item))
        return values

    @staticmethod
    def _point_to_dict(point: Any) -> dict[str, Any]:
        payload = getattr(point, "payload", None) or {}
        vector = getattr(point, "vector", None)
        return {
            "id": getattr(point, "id", None),
            "vector": vector,
            "metadata": dict(payload),
        }

    @staticmethod
    def _scored_point_to_dict(point: Any) -> dict[str, Any]:
        payload = getattr(point, "payload", None) or {}
        return {
            "id": getattr(point, "id", None),
            "score": getattr(point, "score", None),
            "metadata": dict(payload),
        }

    def _build_filter(self, where: Mapping[str, Any] | str | None):
        if where is None:
            return None
        if isinstance(where, str):
            raise QueryError("string filters are not supported for vector adapters; use a mapping")
        if not isinstance(where, Mapping):
            raise QueryError("where must be a mapping or None")
        if not where:
            return None
        self._ensure_models_loaded()
        if self._models is None:
            return {"must": [{"key": str(key), "match": {"value": value}} for key, value in where.items()]}
        conditions = []
        for key, value in where.items():
            if isinstance(value, (list, tuple, set)):
                conditions.append(self._models.FieldCondition(key=str(key), match=self._models.MatchAny(any=list(value))))
            else:
                conditions.append(self._models.FieldCondition(key=str(key), match=self._models.MatchValue(value=value)))
        return self._models.Filter(must=conditions)

    def run_native(self, query: Any, params: Mapping[str, Any] | None = None):
        if params:
            raise QueryError("VectorAdapter does not support SQL-style params")
        if not isinstance(query, Mapping):
            raise QueryError("VectorAdapter native query must be a mapping")
        action = str(query.get("action", "")).strip().lower()
        if action == "upsert_vector":
            return self.upsert_vector(query["entity"], query["id"], query["vector"], query.get("metadata"))
        if action == "search_similar":
            return self.search_similar(query["entity"], query["vector"], top_k=int(query.get("top_k", 10)), filter=query.get("filter"))
        if action == "delete_vector":
            return self.delete_vector(query["entity"], query["id"])
        if action == "create_collection":
            return self.create_collection(query["entity"], int(query["vector_size"]), str(query.get("distance", "cosine")))
        if action == "collection_info":
            return self.collection_info(query["entity"])
        raise QueryError("Unsupported vector native operation")

    def convert_uql(self, uql_query: str):
        return {"action": "uql_passthrough", "uql": uql_query, "db_instance": self.db_instance}

    def create_collection(self, entity: str, vector_size: int, distance: str = "cosine") -> Any:
        collection = self._validate_entity(entity)
        if vector_size <= 0:
            raise QueryError("vector_size must be positive")
        client = self._get_qdrant()
        self._ensure_models_loaded()
        if self._models is None or self._distance_map is None:
            raise ConnectionError("qdrant-client models are required for collection creation")
        distance_value = distance.lower().strip()
        if distance_value not in self._distance_map:
            raise QueryError("distance must be one of: cosine, euclid, dot")
        log_event(self._logger, 20, "Creating vector collection", event="collection.create", db=self.db_instance, entity=collection)
        client.recreate_collection(
            collection_name=collection,
            vectors_config=self._models.VectorParams(size=vector_size, distance=self._distance_map[distance_value]),
        )
        return {"ok": True, "collection": collection, "vector_size": vector_size, "distance": distance_value}

    def collection_info(self, entity: str) -> Any:
        collection = self._validate_entity(entity)
        client = self._get_qdrant()
        info = client.get_collection(collection)
        config = getattr(info, "config", None)
        params = getattr(getattr(config, "params", None), "vectors", None)
        return {
            "collection": collection,
            "vectors_count": getattr(info, "vectors_count", None),
            "points_count": getattr(info, "points_count", None),
            "status": str(getattr(info, "status", "unknown")),
            "distance": str(getattr(params, "distance", "unknown")),
            "vector_size": getattr(params, "size", None),
        }

    def upsert_vector(self, entity: str, id: Any, vector: Any, metadata: Mapping[str, Any] | None = None) -> Any:
        collection = self._validate_entity(entity)
        point_id = id if id is not None else str(uuid4())
        vector_values = self._validate_vector(vector)
        payload = dict(metadata or {})
        client = self._get_qdrant()
        self._ensure_models_loaded()
        if self._models is not None:
            points = [self._models.PointStruct(id=point_id, vector=vector_values, payload=payload)]
            client.upsert(collection_name=collection, points=points)
        else:
            client.upsert(collection_name=collection, points=[{"id": point_id, "vector": vector_values, "payload": payload}])
        return {"id": point_id, "metadata": payload}

    def search_similar(
        self,
        entity: str,
        vector: Any,
        top_k: int = 10,
        filter: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        collection = self._validate_entity(entity)
        vector_values = self._validate_vector(vector)
        if top_k <= 0:
            raise QueryError("top_k must be positive")
        client = self._get_qdrant()
        results = client.search(
            collection_name=collection,
            query_vector=vector_values,
            limit=top_k,
            query_filter=self._build_filter(filter),
        )
        return [self._scored_point_to_dict(point) for point in results]

    def delete_vector(self, entity: str, id: Any) -> Any:
        collection = self._validate_entity(entity)
        if id is None:
            raise QueryError("id is required")
        client = self._get_qdrant()
        self._ensure_models_loaded()
        if self._models is not None:
            selector = self._models.PointIdsList(points=[id])
        else:
            selector = {"points": [id]}
        client.delete(collection_name=collection, points_selector=selector)
        return {"deleted": True, "id": id}

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        if not isinstance(data, Mapping) or not data:
            raise QueryError("data must be a non-empty mapping")
        payload = dict(data)
        vector = payload.pop("vector", None)
        if vector is None:
            raise QueryError("vector field is required for vector create")
        point_id = payload.pop("id", None) or str(uuid4())
        return self.upsert_vector(entity, point_id, vector, payload)

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        if not isinstance(rows, list) or not rows:
            raise QueryError("rows must be a non-empty list")
        results = [self.create(entity, row) for row in rows]
        return {"rows_affected": len(results), "results": results}

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        if order_by is not None:
            raise QueryError("order_by is not supported for vector find")
        collection = self._validate_entity(entity)
        client = self._get_qdrant()
        page_size = limit or 50
        points, _ = client.scroll(collection_name=collection, scroll_filter=self._build_filter(where), limit=page_size)
        return [self._point_to_dict(point) for point in points]

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        collection = self._validate_entity(entity)
        if isinstance(where, Mapping) and "id" in where and len(where) == 1:
            return self.delete_vector(collection, where["id"])
        client = self._get_qdrant()
        self._ensure_models_loaded()
        filter_obj = self._build_filter(where)
        if filter_obj is None:
            raise QueryError("delete requires a non-empty filter")
        if self._models is not None:
            selector = self._models.FilterSelector(filter=filter_obj)
        else:
            selector = {"filter": filter_obj}
        client.delete(collection_name=collection, points_selector=selector)
        return {"deleted": True}

    def update(self, entity: str, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        if not isinstance(where, Mapping) or "id" not in where:
            raise QueryError("vector update currently requires where={'id': ...}")
        payload = dict(data)
        vector = payload.pop("vector", None)
        if vector is None:
            existing = self.find(entity, where={"id": where["id"]}, limit=1)
            if not existing:
                raise QueryError("target vector not found")
            vector = existing[0].get("vector")
        return self.upsert_vector(entity, where["id"], vector, payload)

    def count(self, entity: str, where: Mapping[str, Any] | str | None = None) -> int:
        collection = self._validate_entity(entity)
        client = self._get_qdrant()
        result = client.count(collection_name=collection, count_filter=self._build_filter(where), exact=True)
        return int(getattr(result, "count", result if isinstance(result, int) else 0))

    def ping(self) -> Any:
        if self.db_instance == "qdrant":
            client = self._get_qdrant()
            try:
                info = client.get_collections()
            except Exception as exc:
                log_internal_debug(
                    self._logger,
                    "Vector ping failed",
                    event="connection.ping.internal",
                    db=self.db_instance,
                    exc=exc,
                )
                raise ConnectionError("Database connection failed") from exc
            return {"ok": 1, "collections": len(getattr(info, "collections", []))}
        return {"ok": 1, "db_type": "vector", "db_instance": self.db_instance, "status": "stub"}

    def close(self) -> None:
        client = self._client
        if client is None:
            return
        close_method = getattr(client, "close", None)
        if callable(close_method):
            try:
                close_method()
            except Exception as exc:
                log_internal_debug(
                    self._logger,
                    "Vector client close failed",
                    event="connection.close.internal",
                    db=self.db_instance,
                    exc=exc,
                )
        self._client = None

