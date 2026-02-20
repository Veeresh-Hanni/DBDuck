"""UDOM public API surface.

Architecture:
- UDOM routes operations to backend adapters.
- SQL adapters are SQLAlchemy-backed and transaction-aware.
- Non-SQL adapters keep legacy behavior.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Mapping

from ..adapters.mysql_adapter import MySQLAdapter
from ..adapters.postgres_adapter import PostgresAdapter
from ..adapters.sqlite_adapter import SQLiteAdapter
from ..core.base_adapter import BaseAdapter
from ..core.exceptions import QueryError, TransactionError
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

    _SUPPORTED_DB_TYPES = {"sql", "nosql", "graph", "ai", "vector"}
    _SQL_ENGINES = {"sqlite", "mysql", "postgres", "postgresql"}
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
        self.db_type, self.db_instance = self._normalize_config(db_type, db_instance or server)
        self.url = url or self._default_url(self.db_type, self.db_instance)
        self.options = options
        self.parser = UQLParser()
        self.validator = UQLValidator()
        self.logger = get_logger(options.get("log_level"))
        self.adapter = self.get_adapter()

    def _normalize_config(self, db_type: str, db_instance: str | None) -> tuple[str, str]:
        db_type_value = (db_type or "").lower()
        db_instance_value = (db_instance or "").lower()
        if db_type_value in self._SUPPORTED_DB_TYPES:
            if not db_instance_value:
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
        raise ValueError("Unsupported db_type/db_instance for UDOM")

    def _normalize_instance_alias(self, db_instance: str) -> str:
        aliases = {"postgresql": "postgres", "mongo": "mongodb"}
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
        }
        return defaults.get(db_instance)

    def get_adapter(self) -> BaseAdapter:
        if self.db_type == "sql":
            sql_map = {"sqlite": SQLiteAdapter, "mysql": MySQLAdapter, "postgres": PostgresAdapter}
            adapter_cls = sql_map.get(self.db_instance)
            if adapter_cls is None:
                raise ValueError(f"Unsupported SQL db_instance: {self.db_instance}")
            return adapter_cls(url=self.url, **self.options)
        if self.db_type == "nosql":
            return NoSQLAdapter(db_instance=self.db_instance, url=self.url, **self.options)
        if self.db_type == "graph":
            return GraphAdapter(db_instance=self.db_instance, url=self.url, **self.options)
        if self.db_type == "ai":
            return AIAdapter(db_instance=self.db_instance, url=self.url, **self.options)
        if self.db_type == "vector":
            return VectorAdapter(db_instance=self.db_instance, url=self.url, **self.options)
        raise ValueError(f"Unsupported db_type: {self.db_type}")

    @staticmethod
    def _normalize_entity(entity: str) -> str:
        if not isinstance(entity, str) or not entity.strip():
            raise QueryError("entity must be a non-empty string")
        return entity.strip()

    def query(self, query: str) -> Any:
        return self.adapter.run_native(query)

    def execute(self, query: str) -> Any:
        return self.adapter.run_native(query)

    def uquery(self, uql: str) -> str:
        return self.adapter.convert_uql(uql)

    def uexecute(self, uql: str) -> Any:
        valid = self.validator.validate(uql)
        if not valid.get("valid"):
            raise QueryError(valid.get("error", "Invalid UQL"))
        native_query = self.adapter.convert_uql(uql)
        return self.adapter.run_native(native_query)

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        entity_name = self._normalize_entity(entity)
        if self.db_type == "sql":
            log_event(self.logger, 20, "Create request", event="query.create", db=self.db_instance, entity=entity_name)
            return self.adapter.create(entity_name, data)
        body = ", ".join([f"{k}: {self._to_uql_value(v)}" for k, v in data.items()])
        return self.uexecute(f"CREATE {entity_name} " + "{" + body + "}")

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        entity_name = self._normalize_entity(entity)
        if self.db_type == "sql":
            return self.adapter.create_many(entity_name, rows)
        results = []
        for row in rows:
            results.append(self.create(entity_name, row))
        return results

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        entity_name = self._normalize_entity(entity)
        if self.db_type == "sql":
            log_event(self.logger, 20, "Find request", event="query.find", db=self.db_instance, entity=entity_name)
            return self.adapter.find(entity_name, where=where, order_by=order_by, limit=limit)
        return self.uexecute(self._build_find_uql(entity_name, where, order_by, limit))

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        entity_name = self._normalize_entity(entity)
        if self.db_type == "sql":
            log_event(self.logger, 20, "Delete request", event="query.delete", db=self.db_instance, entity=entity_name)
            return self.adapter.delete(entity_name, where=where)
        where_clause = self._to_uql_where(where)
        if not where_clause:
            raise QueryError("delete requires a non-empty where condition")
        return self.uexecute(f"DELETE {entity_name} WHERE {where_clause}")

    def begin(self):
        if self.db_type != "sql":
            raise TransactionError(f"Transactions are not supported for db_type={self.db_type}")
        log_event(self.logger, 20, "Begin transaction", event="transaction.begin", db=self.db_instance)
        return self.adapter.begin()

    def commit(self) -> None:
        if self.db_type != "sql":
            raise TransactionError(f"Transactions are not supported for db_type={self.db_type}")
        log_event(self.logger, 20, "Commit transaction", event="transaction.commit", db=self.db_instance)
        self.adapter.commit()

    def rollback(self) -> None:
        if self.db_type != "sql":
            raise TransactionError(f"Transactions are not supported for db_type={self.db_type}")
        log_event(self.logger, 20, "Rollback transaction", event="transaction.rollback", db=self.db_instance)
        self.adapter.rollback()

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        if self.db_type != "sql":
            raise TransactionError(f"Transactions are not supported for db_type={self.db_type}")
        with self.adapter.transaction():
            yield self

    def usave(self, model: UModel) -> Any:
        table = model.get_name()
        fields = model.get_fields()
        data = {f: getattr(model, f) for f in fields if hasattr(model, f)}
        return self.create(table, data)

    def ufind(self, model: UModel, where: Mapping[str, Any] | str | None = None) -> Any:
        return self.find(model.get_name(), where=where)

    def udelete(self, model: UModel, where: Mapping[str, Any] | str) -> Any:
        return self.delete(model.get_name(), where=where)

    @staticmethod
    def _to_uql_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        text = str(value).replace("'", "\\'")
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
