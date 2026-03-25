"""UDOM public API surface.

Architecture:
- UDOM routes operations to backend adapters.
- SQL adapters are SQLAlchemy-backed and transaction-aware.
- Non-SQL adapters keep legacy behavior.
"""

from __future__ import annotations

from contextlib import contextmanager
import threading
import re
from typing import Any, Iterator, Mapping

from ..core import (
    AdapterRouter,
    SchemaValidator,
    SecurityAuditor,
    SecurityRateLimiter,
    SensitiveFieldProtector,
    load_runtime_settings,
)
from ..core.base_adapter import BaseAdapter
from ..core.exceptions import ConnectionError, QueryError, TransactionError
from ..utils.logger import get_logger, log_event
from .adapters.ai_adapter import AIAdapter
from .adapters.graph_adapter import GraphAdapter
from .adapters.nosql_adapter import NoSQLAdapter
from .adapters.vector_adapter import VectorAdapter
from .models.umodel import UModel
from .uql.uql_parser import UQLParser
from .utils.validator import UQLValidator


class UDOM:
    """Universal Data Object Model across multiple backend categories."""

    _MAX_PAGE = 10_000
    _SUPPORTED_DB_TYPES = {"sql", "nosql", "graph", "ai", "vector"}
    _SQL_ENGINES = {"sqlite", "mysql", "postgres", "postgresql", "mssql", "sqlserver"}
    _NOSQL_ENGINES = {"mongodb", "mongo", "redis", "dynamodb", "firestore", "cassandra"}
    _GRAPH_ENGINES = {"neo4j", "tigergraph", "rdf"}
    _VECTOR_ENGINES = {"qdrant", "pinecone", "weaviate", "milvus", "chroma", "pgvector"}
    _AI_ENGINES = {"openai", "azure-openai", "bedrock", "vertexai", "ollama"}

    def __init__(
        self,
        db_type: str = "sql",
        db_instance: str | None = None,
        server: str | None = None,
        url: str | None = None,
        **options: Any,
    ) -> None:
        self.db_type, self.db_instance = self._normalize_config(db_type, db_instance or server, url)
        self.url = url or self._default_url(self.db_type, self.db_instance)
        self.options = options
        self.settings = load_runtime_settings(**options)
        self.adapter_options = {**self.settings.as_adapter_options(), **options}
        self.parser = UQLParser()
        self.validator = UQLValidator()
        self.logger = get_logger(self.adapter_options.get("log_level"))
        self.adapter = self.get_adapter()
        self._rate_limiter = SecurityRateLimiter(
            enabled=self.settings.rate_limit_enabled,
            max_requests=self.settings.rate_limit_max_requests,
            window_seconds=self.settings.rate_limit_window_seconds,
        )
        self._auditor = SecurityAuditor(
            enabled=self.settings.security_audit_enabled,
            entity_name=self.settings.security_audit_entity,
        )
        self._security_lock = threading.Lock()

    def _normalize_config(self, db_type: str, db_instance: str | None, url: str | None) -> tuple[str, str]:
        db_type_value = (db_type or "").lower()
        db_instance_value = (db_instance or "").lower()
        if db_type_value in self._SUPPORTED_DB_TYPES:
            if not db_instance_value:
                if db_type_value == "sql":
                    db_instance_value = AdapterRouter.infer_sql_instance_from_url(url) or self._default_instance(
                        db_type_value
                    )
                else:
                    db_instance_value = self._default_instance(db_type_value)
            return db_type_value, self._normalize_instance_alias(db_instance_value)
        engine = self._normalize_instance_alias(db_type_value)
        if engine in self._SQL_ENGINES:
            return "sql", "postgres" if engine == "postgresql" else engine
        if engine in self._NOSQL_ENGINES:
            return "nosql", "mongodb" if engine == "mongo" else engine
        if engine in self._GRAPH_ENGINES:
            return "graph", engine
        if engine in self._VECTOR_ENGINES:
            return "vector", engine
        if engine in self._AI_ENGINES:
            return "ai", engine
        raise ConnectionError("Unsupported db_type/db_instance for UDOM")

    def _normalize_instance_alias(self, db_instance: str) -> str:
        aliases = {
            "postgresql": "postgres",
            "postgres": "postgres",
            "psql": "postgres",
            "pg": "postgres",
            "mongo": "mongodb",
            "mongod": "mongodb",
            "sqlserver": "mssql",
            "ms-sql": "mssql",
        }
        return aliases.get(db_instance, db_instance)

    def _default_instance(self, db_type: str) -> str:
        defaults = {
            "sql": "sqlite",
            "nosql": "mongodb",
            "graph": "neo4j",
            "vector": "qdrant",
            "ai": "openai",
        }
        return defaults[db_type]

    def _default_url(self, db_type: str, db_instance: str) -> str | None:
        if db_type != "sql":
            return None
        defaults = {
            "sqlite": "sqlite:///test.db",
            "mysql": "mysql+pymysql://root:password@localhost:3306/udom",
            "postgres": "postgresql+psycopg2://postgres:password@localhost:5432/udom",
            "mssql": "mssql+pyodbc://sa:password@localhost:1433/udom?driver=ODBC+Driver+17+for+SQL+Server",
        }
        return defaults.get(db_instance)

    def get_adapter(self) -> BaseAdapter:
        if self.db_type == "sql":
            self.db_instance, adapter_cls = AdapterRouter.route_sql_adapter(self.db_instance, self.url)
            return adapter_cls(url=self.url, **self.adapter_options)
        if self.db_type == "nosql":
            return NoSQLAdapter(db_instance=self.db_instance, url=self.url, **self.adapter_options)
        if self.db_type == "graph":
            return GraphAdapter(db_instance=self.db_instance, url=self.url, **self.adapter_options)
        if self.db_type == "ai":
            return AIAdapter(db_instance=self.db_instance, url=self.url, **self.adapter_options)
        if self.db_type == "vector":
            return VectorAdapter(db_instance=self.db_instance, url=self.url, **self.adapter_options)
        raise ConnectionError(f"Unsupported db_type: {self.db_type}")

    @staticmethod
    def _normalize_entity(entity: str) -> str:
        if not isinstance(entity, str) or not entity.strip():
            raise QueryError("entity must be a non-empty string")
        return entity.strip()

    def query(self, query: str) -> Any:
        self._enforce_rate_limit("query", entity="-", caller_id=self._current_caller_id())
        return self.adapter.run_native(query)

    def execute(self, query: str) -> Any:
        self._enforce_rate_limit("execute", entity="-", caller_id=self._current_caller_id())
        return self.adapter.run_native(query)

    def uquery(self, uql: str) -> str:
        return self.adapter.convert_uql(uql)

    def uexecute(self, uql: str) -> Any:
        self._enforce_rate_limit("uexecute", entity="-", caller_id=self._current_caller_id())
        valid = self.validator.validate(uql)
        if not valid.get("valid"):
            error = QueryError(valid.get("error", "Invalid UQL"))
            self._audit_security_error("uexecute", "-", uql, error)
            raise error
        native_query = self.adapter.convert_uql(uql)
        if isinstance(native_query, tuple) and len(native_query) == 2:
            query, params = native_query
            if not isinstance(params, Mapping):
                raise QueryError("Invalid converted UQL parameters")
            return self.adapter.run_native(query, params=params)
        return self.adapter.run_native(native_query)

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        payload = SchemaValidator.validate_create_data(data)
        return self._create_internal(entity_name, payload, sensitive_fields=None)

    def _create_internal(
        self,
        entity_name: str,
        payload: Mapping[str, Any],
        *,
        sensitive_fields: set[str] | None,
    ) -> Any:
        self._enforce_rate_limit("create", entity=entity_name, caller_id=self._current_caller_id())
        payload = self._protect_sensitive_payload(payload, field_names=sensitive_fields)
        if self.db_type in {"sql", "nosql"}:
            log_event(self.logger, 20, "Create request", event="query.create", db=self.db_instance, entity=entity_name)
            try:
                return self.adapter.create(entity_name, payload)
            except QueryError as exc:
                self._audit_security_error("create", entity_name, payload, exc)
                raise
        body = ", ".join([f"{k}: {self._to_uql_value(v)}" for k, v in payload.items()])
        return self.uexecute(f"CREATE {entity_name} " + "{" + body + "}")

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        if not isinstance(rows, list) or not rows:
            raise QueryError("rows must be a non-empty list")
        return self._create_many_internal(entity_name, rows, sensitive_fields=None)

    def _create_many_internal(
        self,
        entity_name: str,
        rows: list[Mapping[str, Any]],
        *,
        sensitive_fields: set[str] | None,
    ) -> Any:
        self._enforce_rate_limit("create_many", entity=entity_name, caller_id=self._current_caller_id())
        if self.db_type in {"sql", "nosql"}:
            payloads = [
                self._protect_sensitive_payload(SchemaValidator.validate_create_data(row), field_names=sensitive_fields)
                for row in rows
            ]
            try:
                return self.adapter.create_many(entity_name, payloads)
            except QueryError as exc:
                self._audit_security_error("create_many", entity_name, payloads, exc)
                raise
        results = []
        for row in rows:
            results.append(self.create(entity_name, SchemaValidator.validate_create_data(row)))
        return results

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        self._enforce_rate_limit("find", entity=entity_name, caller_id=self._current_caller_id())
        try:
            where = SchemaValidator.validate_find_where(where)
        except QueryError as exc:
            self._audit_security_error("find", entity_name, where, exc)
            raise
        if self.db_type in {"sql", "nosql"}:
            log_event(self.logger, 20, "Find request", event="query.find", db=self.db_instance, entity=entity_name)
            try:
                return self.adapter.find(entity_name, where=where, order_by=order_by, limit=limit)
            except QueryError as exc:
                self._audit_security_error("find", entity_name, {"where": where, "order_by": order_by, "limit": limit}, exc)
                raise
        return self.uexecute(self._build_find_uql(entity_name, where, order_by, limit))

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        self._enforce_rate_limit("delete", entity=entity_name, caller_id=self._current_caller_id())
        try:
            where = SchemaValidator.validate_find_where(where)
        except QueryError as exc:
            self._audit_security_error("delete", entity_name, where, exc)
            raise
        if self.db_type in {"sql", "nosql"}:
            log_event(self.logger, 20, "Delete request", event="query.delete", db=self.db_instance, entity=entity_name)
            try:
                return self.adapter.delete(entity_name, where=where)
            except QueryError as exc:
                self._audit_security_error("delete", entity_name, where, exc)
                raise
        where_clause = self._to_uql_where(where)
        if not where_clause:
            raise QueryError("delete requires a non-empty where condition")
        return self.uexecute(f"DELETE {entity_name} WHERE {where_clause}")

    def update(self, entity: str, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        payload = SchemaValidator.validate_create_data(data)
        return self._update_internal(entity_name, payload, where, sensitive_fields=None)

    def _update_internal(
        self,
        entity_name: str,
        payload: Mapping[str, Any],
        where: Mapping[str, Any] | str,
        *,
        sensitive_fields: set[str] | None,
    ) -> Any:
        self._enforce_rate_limit("update", entity=entity_name, caller_id=self._current_caller_id())
        payload = self._protect_sensitive_payload(payload, field_names=sensitive_fields)
        try:
            where = SchemaValidator.validate_find_where(where)
        except QueryError as exc:
            self._audit_security_error("update", entity_name, {"data": payload, "where": where}, exc)
            raise
        if self.db_type in {"sql", "nosql"}:
            log_event(self.logger, 20, "Update request", event="query.update", db=self.db_instance, entity=entity_name)
            try:
                return self.adapter.update(entity_name, payload, where=where)
            except QueryError as exc:
                self._audit_security_error("update", entity_name, {"data": payload, "where": where}, exc)
                raise
        raise QueryError("update is currently supported for SQL and NoSQL adapters")

    def count(self, entity: str, where: Mapping[str, Any] | str | None = None) -> int:
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        self._enforce_rate_limit("count", entity=entity_name, caller_id=self._current_caller_id())
        try:
            where = SchemaValidator.validate_find_where(where) if where is not None else None
        except QueryError as exc:
            self._audit_security_error("count", entity_name, where, exc)
            raise
        if self.db_type in {"sql", "nosql"}:
            return int(self.adapter.count(entity_name, where=where))
        raise QueryError("count is currently supported for SQL and NoSQL adapters")

    def aggregate(
        self,
        entity: str,
        *,
        group_by: str | list[str] | tuple[str, ...] | None = None,
        metrics: Mapping[str, Any] | None = None,
        where: Mapping[str, Any] | str | None = None,
        having: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        pipeline: list[Mapping[str, Any]] | None = None,
    ) -> Any:
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        self._enforce_rate_limit("aggregate", entity=entity_name, caller_id=self._current_caller_id())
        try:
            where = SchemaValidator.validate_find_where(where) if where is not None else None
            having = SchemaValidator.validate_find_where(having) if having is not None else None
        except QueryError as exc:
            self._audit_security_error(
                "aggregate",
                entity_name,
                {"where": where, "having": having, "group_by": group_by, "metrics": metrics},
                exc,
            )
            raise
        if self.db_type in {"sql", "nosql"}:
            return self.adapter.aggregate(
                entity_name,
                group_by=group_by,
                metrics=metrics,
                where=where,
                having=having,
                order_by=order_by,
                limit=limit,
                pipeline=pipeline,
            )
        raise QueryError("aggregate is currently supported for SQL and NoSQL adapters")

    def find_page(
        self,
        entity: str,
        *,
        page: int = 1,
        page_size: int = 20,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        if page <= 0:
            raise QueryError("page must be >= 1")
        if page > self._MAX_PAGE:
            raise QueryError(f"page number cannot exceed {self._MAX_PAGE}. Use cursor-based pagination for large offsets.")
        if page_size <= 0:
            raise QueryError("page_size must be >= 1")
        if page_size > 1000:
            raise QueryError("page_size too large")
        total = self.count(entity, where=where)
        limit = page_size
        offset = (page - 1) * page_size
        if offset > 10_000_000:
            raise QueryError("Requested offset too large. Use cursor-based pagination.")
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        if hasattr(self.adapter, "paginate"):
            items = self.adapter.paginate(
                entity_name,
                where=where,
                order_by=order_by,
                limit=limit,
                offset=offset,
            )
        else:
            items = self.find(entity_name, where=where, order_by=order_by, limit=offset + limit)
            if isinstance(items, list):
                items = items[offset : offset + limit]
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0
        return {
            "items": items if isinstance(items, list) else [],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    def begin(self):
        if self.db_type not in {"sql", "nosql"}:
            raise TransactionError(f"Transactions are not supported for db_type={self.db_type}")
        log_event(self.logger, 20, "Begin transaction", event="transaction.begin", db=self.db_instance)
        return self.adapter.begin()

    def commit(self) -> None:
        if self.db_type not in {"sql", "nosql"}:
            raise TransactionError(f"Transactions are not supported for db_type={self.db_type}")
        log_event(self.logger, 20, "Commit transaction", event="transaction.commit", db=self.db_instance)
        self.adapter.commit()

    def rollback(self) -> None:
        if self.db_type not in {"sql", "nosql"}:
            raise TransactionError(f"Transactions are not supported for db_type={self.db_type}")
        log_event(self.logger, 20, "Rollback transaction", event="transaction.rollback", db=self.db_instance)
        self.adapter.rollback()

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        if self.db_type not in {"sql", "nosql"}:
            raise TransactionError(f"Transactions are not supported for db_type={self.db_type}")
        with self.adapter.transaction():
            yield self

    def ping(self) -> Any:
        if hasattr(self.adapter, "ping"):
            return self.adapter.ping()
        return self.execute("ping")

    def close(self) -> None:
        if hasattr(self.adapter, "close"):
            self.adapter.close()

    def ensure_indexes(self, entity: str, indexes: list[Mapping[str, Any]]) -> Any:
        entity_name = SchemaValidator.validate_entity(self._normalize_entity(entity))
        if self.db_type != "nosql":
            raise QueryError("ensure_indexes is currently supported for NoSQL adapters only")
        if not hasattr(self.adapter, "ensure_indexes"):
            raise QueryError("Current adapter does not support ensure_indexes")
        return self.adapter.ensure_indexes(entity_name, indexes)

    def create_relationship(
        self,
        from_label: str,
        from_id: Any,
        rel_type: str,
        to_label: str,
        to_id: Any,
        props: Mapping[str, Any] | None = None,
    ) -> Any:
        if self.db_type != "graph" or not hasattr(self.adapter, "create_relationship"):
            raise QueryError("create_relationship is currently supported for graph adapters only")
        return self.adapter.create_relationship(from_label, from_id, rel_type, to_label, to_id, props)

    def find_related(
        self,
        entity: str,
        id: Any,
        rel_type: str,
        direction: str = "out",
        target_label: str | None = None,
    ) -> Any:
        if self.db_type != "graph" or not hasattr(self.adapter, "find_related"):
            raise QueryError("find_related is currently supported for graph adapters only")
        return self.adapter.find_related(entity, id, rel_type, direction, target_label)

    def shortest_path(self, from_label: str, from_id: Any, to_label: str, to_id: Any) -> Any:
        if self.db_type != "graph" or not hasattr(self.adapter, "shortest_path"):
            raise QueryError("shortest_path is currently supported for graph adapters only")
        return self.adapter.shortest_path(from_label, from_id, to_label, to_id)

    def create_collection(self, entity: str, vector_size: int, distance: str = "cosine") -> Any:
        if self.db_type != "vector" or not hasattr(self.adapter, "create_collection"):
            raise QueryError("create_collection is currently supported for vector adapters only")
        return self.adapter.create_collection(entity, vector_size, distance)

    def collection_info(self, entity: str) -> Any:
        if self.db_type != "vector" or not hasattr(self.adapter, "collection_info"):
            raise QueryError("collection_info is currently supported for vector adapters only")
        return self.adapter.collection_info(entity)

    def upsert_vector(self, entity: str, id: Any, vector: Any, metadata: Mapping[str, Any] | None = None) -> Any:
        if self.db_type != "vector" or not hasattr(self.adapter, "upsert_vector"):
            raise QueryError("upsert_vector is currently supported for vector adapters only")
        return self.adapter.upsert_vector(entity, id, vector, metadata)

    def search_similar(
        self,
        entity: str,
        vector: Any,
        top_k: int = 10,
        filter: Mapping[str, Any] | None = None,
    ) -> Any:
        if self.db_type != "vector" or not hasattr(self.adapter, "search_similar"):
            raise QueryError("search_similar is currently supported for vector adapters only")
        return self.adapter.search_similar(entity, vector, top_k, filter)

    def delete_vector(self, entity: str, id: Any) -> Any:
        if self.db_type != "vector" or not hasattr(self.adapter, "delete_vector"):
            raise QueryError("delete_vector is currently supported for vector adapters only")
        return self.adapter.delete_vector(entity, id)

    def create_view(self, name: str, select_query: str, *, replace: bool = False) -> Any:
        view_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("create_view is currently supported for SQL adapters only")
        return self.adapter.create_view(view_name, select_query, replace=replace)

    def drop_view(self, name: str, *, if_exists: bool = True) -> Any:
        view_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("drop_view is currently supported for SQL adapters only")
        return self.adapter.drop_view(view_name, if_exists=if_exists)

    def create_procedure(self, name: str, definition: str, *, replace: bool = False) -> Any:
        proc_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("create_procedure is currently supported for SQL adapters only")
        return self.adapter.create_procedure(proc_name, definition, replace=replace)

    def drop_procedure(self, name: str, *, if_exists: bool = True) -> Any:
        proc_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("drop_procedure is currently supported for SQL adapters only")
        return self.adapter.drop_procedure(proc_name, if_exists=if_exists)

    def call_procedure(self, name: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        proc_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("call_procedure is currently supported for SQL adapters only")
        return self.adapter.call_procedure(proc_name, params=params)

    def create_function(self, name: str, definition: str, *, replace: bool = False) -> Any:
        func_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("create_function is currently supported for SQL adapters only")
        return self.adapter.create_function(func_name, definition, replace=replace)

    def drop_function(self, name: str, *, if_exists: bool = True) -> Any:
        func_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("drop_function is currently supported for SQL adapters only")
        return self.adapter.drop_function(func_name, if_exists=if_exists)

    def call_function(self, name: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        func_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("call_function is currently supported for SQL adapters only")
        return self.adapter.call_function(func_name, params=params)

    def create_event(
        self,
        name: str,
        schedule: str,
        body: str,
        *,
        replace: bool = False,
        preserve: bool = True,
        enable: bool = True,
    ) -> Any:
        event_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("create_event is currently supported for SQL adapters only")
        return self.adapter.create_event(
            event_name,
            schedule,
            body,
            replace=replace,
            preserve=preserve,
            enable=enable,
        )

    def drop_event(self, name: str, *, if_exists: bool = True) -> Any:
        event_name = SchemaValidator.validate_entity(self._normalize_entity(name))
        if self.db_type != "sql":
            raise QueryError("drop_event is currently supported for SQL adapters only")
        return self.adapter.drop_event(event_name, if_exists=if_exists)

    def verify_secret(self, plain_value: Any, stored_hash: Any) -> bool:
        """Validate plaintext secret against a stored BCrypt hash."""
        return SensitiveFieldProtector.verify_secret(plain_value, stored_hash)

    def __enter__(self) -> "UDOM":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def usave(self, model: UModel) -> Any:
        table = model.get_name()
        fields = model.get_fields()
        data = {f: getattr(model, f) for f in fields if hasattr(model, f)}
        return self.create(table, data)

    def ufind(self, model: UModel, where: Mapping[str, Any] | str | None = None) -> Any:
        return self.find(model.get_name(), where=where)

    def udelete(self, model: UModel, where: Mapping[str, Any] | str) -> Any:
        return self.delete(model.get_name(), where=where)

    def uupdate(self, model: UModel, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        return self.update(model.get_name(), data=data, where=where)

    def uaggregate(
        self,
        model: UModel | type[UModel],
        *,
        group_by: str | list[str] | tuple[str, ...] | None = None,
        metrics: Mapping[str, Any] | None = None,
        where: Mapping[str, Any] | str | None = None,
        having: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        pipeline: list[Mapping[str, Any]] | None = None,
    ) -> Any:
        model_cls = model if isinstance(model, type) else model.__class__
        return self.aggregate(
            model_cls.get_name(),
            group_by=group_by,
            metrics=metrics,
            where=where,
            having=having,
            order_by=order_by,
            limit=limit,
            pipeline=pipeline,
        )

    def uensure_indexes(self, model_cls: type[UModel]) -> Any:
        indexes = getattr(model_cls, "__indexes__", None)
        if indexes is None:
            raise QueryError("Model has no __indexes__ definition")
        return self.ensure_indexes(model_cls.get_name(), indexes)

    @staticmethod
    def _to_uql_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        text = re.sub(r"[\x00-\x1f\x7f]", "", str(value)).replace("'", "''")
        return f"'{text}'"

    def _to_uql_where(self, where: Mapping[str, Any] | str | None) -> str | None:
        if where is None:
            return None
        if isinstance(where, str):
            text = where.strip()
            return text if text else None
        if isinstance(where, Mapping):
            parts = [f"{k} = {self._to_uql_value(v)}" for k, v in where.items()]
            return " AND ".join(parts) if parts else None
        raise QueryError("where must be a string, mapping, or None")

    def _build_find_uql(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> str:
        uql = f"FIND {entity}"
        where_clause = self._to_uql_where(where)
        if where_clause:
            uql += f" WHERE {where_clause}"
        if order_by:
            uql += f" ORDER BY {order_by}"
        if limit is not None:
            uql += f" LIMIT {int(limit)}"
        return uql

    def _protect_sensitive_payload(
        self,
        payload: Mapping[str, Any],
        *,
        field_names: set[str] | None,
    ) -> dict[str, Any]:
        return SensitiveFieldProtector.protect_mapping(
            payload,
            enabled=self.settings.hash_sensitive_fields,
            rounds=self.settings.bcrypt_rounds,
            field_names=field_names,
        )

    def _current_caller_id(self) -> str:
        return str(self.options.get("caller_id", "global")) or "global"

    def _enforce_rate_limit(self, operation: str, *, entity: str, caller_id: str = "global") -> None:
        if operation in {"create", "create_many", "update", "delete"} and entity.lower() == self.settings.security_audit_entity.lower():
            raise QueryError("Direct writes to the security audit entity are not permitted")
        decision = self._rate_limiter.check(f"{caller_id}:{operation}:{entity}")
        if decision.allowed:
            return
        error = QueryError("Rate limit exceeded")
        self._audit_security_error(
            operation,
            entity,
            {"retry_after_seconds": round(decision.retry_after_seconds, 3)},
            error,
        )
        raise error

    def _audit_security_error(self, operation: str, entity: str, input_data: Any, exc: QueryError) -> None:
        reason = str(exc)
        if not self._is_security_relevant_error(reason):
            return
        with self._security_lock:
            self._auditor.record(
                adapter=self.adapter,
                logger=self.logger,
                db_type=self.db_type,
                db_instance=self.db_instance,
                operation=operation,
                entity=entity,
                reason=reason,
                input_data=input_data,
            )

    @staticmethod
    def _is_security_relevant_error(message: str) -> bool:
        text = message.lower()
        markers = (
            "potential injection",
            "operator expressions are not allowed",
            "rate limit exceeded",
            "invalid integer value",
            "invalid numeric value",
            "invalid boolean value",
            "valid identifier",
        )
        return any(marker in text for marker in markers)
