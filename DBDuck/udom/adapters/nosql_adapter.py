import re
import threading
import time
from contextlib import contextmanager
from typing import Any, Mapping
from urllib.parse import urlparse

from ...core import MongoConnectionManager
from ...core.exceptions import ConnectionError, QueryError, TransactionError
from ...utils.logger import get_logger, log_event, log_internal_debug
from .base_adapter import BaseAdapter


class NoSQLAdapter(BaseAdapter):
    """Adapter for NoSQL databases (MongoDB-style BSON format)."""

    _DANGEROUS = re.compile(
        r"(?:--|/\*|\*/|;|\$[a-zA-Z]|\b(where|drop|truncate|union|exec|eval|function)\b)",
        re.IGNORECASE,
    )
    _PUBLIC_QUERY_ERROR = "Database execution failed"
    _PUBLIC_CONNECTION_ERROR = "Database connection failed"

    def __init__(self, db_instance="mongodb", url="mongodb://localhost:27017/udom", **options):
        self.db_instance = db_instance
        self.url = url or "mongodb://localhost:27017/udom"
        self.options = options
        self._retry_attempts = max(1, int(options.get("retry_attempts", 3)))
        self._retry_backoff_ms = max(0, int(options.get("retry_backoff_ms", 100)))
        self._client = None
        self._db = None
        self._local = threading.local()
        self._logger = get_logger(options.get("log_level"))
        self._conn_manager = MongoConnectionManager()
        self._logger = get_logger(options.get("log_level"))
        self._conn_manager._logger = self._logger

    def run_native(self, query: Any, params: Mapping[str, Any] | None = None):
        """Execute Mongo operation dicts when db_instance is mongodb."""
        if params:
            raise QueryError("NoSQLAdapter does not support SQL-style params argument")

        try:
            if self.db_instance != "mongodb":
                return query

            self._ensure_mongo()
            session = self._active_session()

            if isinstance(query, str):
                cmd = query.strip().lower()
                if cmd in {"show dbs", "show databases"}:
                    return [db.get("name") for db in self._client.list_databases()]
                if cmd == "ping":
                    return self._db.command("ping")
                raise QueryError("Unsupported Mongo string command. Use dict operations or 'show dbs'/'ping'.")

            if not isinstance(query, dict):
                raise QueryError("Mongo native query must be a dict operation")

            if "find" in query:
                collection = query["find"]
                where = query.get("where", {})
                cursor = self._run_with_retry(
                    lambda: self._db[collection].find(where, session=session),
                    operation="find",
                    entity=collection,
                )
                order_by = query.get("order_by")
                if order_by:
                    cursor = cursor.sort(order_by)
                limit = query.get("limit")
                if isinstance(limit, int) and limit > 0:
                    cursor = cursor.limit(limit)
                offset = query.get("offset")
                if isinstance(offset, int) and offset > 0:
                    cursor = cursor.skip(offset)
                return [self._serialize_doc(doc) for doc in cursor]

            if "aggregate" in query:
                collection = query["aggregate"]
                pipeline = query.get("pipeline", [])
                if not isinstance(pipeline, list):
                    raise QueryError("aggregate pipeline must be a list")
                cursor = self._run_with_retry(
                    lambda: self._db[collection].aggregate(pipeline, session=session),
                    operation="aggregate",
                    entity=collection,
                )
                return [self._serialize_doc(doc) for doc in cursor]

            if "insert" in query:
                collection = query["insert"]
                document = query.get("document", {})
                result = self._run_with_retry(
                    lambda: self._db[collection].insert_one(document, session=session),
                    operation="insert_one",
                    entity=collection,
                )
                return {"inserted_id": str(result.inserted_id)}

            if "insert_many" in query:
                collection = query["insert_many"]
                documents = query.get("documents", [])
                result = self._run_with_retry(
                    lambda: self._db[collection].insert_many(documents, ordered=True, session=session),
                    operation="insert_many",
                    entity=collection,
                )
                return {"inserted_count": len(result.inserted_ids)}

            if "delete" in query:
                collection = query["delete"]
                where = query.get("where", {})
                result = self._run_with_retry(
                    lambda: self._db[collection].delete_many(where, session=session),
                    operation="delete_many",
                    entity=collection,
                )
                return {"deleted_count": result.deleted_count}

            if "update" in query:
                collection = query["update"]
                where = query.get("where", {})
                values = query.get("values", {})
                result = self._run_with_retry(
                    lambda: self._db[collection].update_many(where, {"$set": values}, session=session),
                    operation="update_many",
                    entity=collection,
                )
                return {"matched_count": result.matched_count, "modified_count": result.modified_count}

            raise QueryError("Unsupported Mongo operation")
        except (QueryError, ConnectionError):
            raise
        except Exception as exc:
            self._log_error("query.error", "Mongo operation failed", exc)
            if self._is_transient_mongo_error(exc):
                raise ConnectionError(self._PUBLIC_CONNECTION_ERROR) from exc
            raise QueryError(self._PUBLIC_QUERY_ERROR) from exc

    def convert_uql(self, uql_query: str):
        """Convert basic UQL commands to Mongo-style operation dictionaries."""
        uql_query = uql_query.strip()
        cmd = uql_query.upper()

        if cmd.startswith("FIND"):
            collection, condition = self._extract_collection_and_condition(uql_query)
            return {"find": collection.lower(), "where": self._convert_condition(condition)}

        if cmd.startswith("DELETE"):
            collection, condition = self._extract_collection_and_condition(uql_query)
            return {"delete": collection.lower(), "where": self._convert_condition(condition)}

        if cmd.startswith("CREATE"):
            match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", uql_query, re.IGNORECASE)
            if match:
                collection = match.group(1).lower()
                fields = match.group(2)
                return {"insert": collection, "document": self._parse_key_value_pairs(fields)}

        raise QueryError("Unsupported or invalid UQL syntax")

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        if not isinstance(data, Mapping) or not data:
            raise QueryError("data must be a non-empty mapping")
        log_event(self._logger, 20, "Mongo create", event="query.create", db=self.db_instance, entity=entity)
        return self.run_native({"insert": entity.lower(), "document": dict(data)})

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        if not isinstance(rows, list) or not rows:
            raise QueryError("rows must be a non-empty list")
        for row in rows:
            if not isinstance(row, Mapping) or not row:
                raise QueryError("each row must be a non-empty mapping")
        log_event(self._logger, 20, "Mongo create_many", event="query.create", db=self.db_instance, entity=entity)
        if self.db_instance == "mongodb":
            return self.run_native({"insert_many": entity.lower(), "documents": [dict(r) for r in rows]})
        return {"inserted_count": len(rows), "results": [self.create(entity, r) for r in rows]}

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return self.paginate(entity, where=where, order_by=order_by, limit=limit, offset=0)

    def paginate(
        self,
        entity: str,
        *,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> Any:
        native_where = self._normalize_where(where)
        mongo_sort = self._parse_order_by(order_by) if order_by else None
        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise QueryError("limit must be a positive integer")
        if not isinstance(offset, int) or offset < 0:
            raise QueryError("offset must be a non-negative integer")
        log_event(self._logger, 20, "Mongo find", event="query.find", db=self.db_instance, entity=entity)
        return self.run_native(
            {
                "find": entity.lower(),
                "where": native_where,
                "order_by": mongo_sort,
                "limit": limit,
                "offset": offset,
            }
        )

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        native_where = self._normalize_where(where)
        if not native_where:
            raise QueryError("delete requires a non-empty where condition")
        log_event(self._logger, 20, "Mongo delete", event="query.delete", db=self.db_instance, entity=entity)
        return self.run_native({"delete": entity.lower(), "where": native_where})

    def update(self, entity: str, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        if not isinstance(data, Mapping) or not data:
            raise QueryError("update data must be a non-empty mapping")
        native_where = self._normalize_where(where)
        if not native_where:
            raise QueryError("update requires a non-empty where condition")
        log_event(self._logger, 20, "Mongo update", event="query.update", db=self.db_instance, entity=entity)
        return self.run_native({"update": entity.lower(), "where": native_where, "values": dict(data)})

    def count(self, entity: str, where: Mapping[str, Any] | str | None = None) -> int:
        native_where = self._normalize_where(where)
        if self.db_instance != "mongodb":
            return 0
        self._ensure_mongo()
        collection = self._db[entity.lower()]
        total = self._run_with_retry(
            lambda: collection.count_documents(native_where, session=self._active_session()),
            operation="count_documents",
            entity=entity,
        )
        return int(total)

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
        if self.db_instance != "mongodb":
            return self.run_native(
                {
                    "action": "aggregate",
                    "entity": entity,
                    "group_by": group_by,
                    "metrics": dict(metrics or {}),
                    "where": where,
                    "having": having,
                    "order_by": order_by,
                    "limit": limit,
                    "pipeline": pipeline or [],
                }
            )

        if pipeline is not None:
            if not isinstance(pipeline, list):
                raise QueryError("pipeline must be a list of Mongo aggregation stages")
            built_pipeline = list(pipeline)
        else:
            built_pipeline = self._build_aggregate_pipeline(
                group_by=group_by,
                metrics=metrics,
                where=where,
                having=having,
                order_by=order_by,
                limit=limit,
            )
        return self.run_native({"aggregate": entity.lower(), "pipeline": built_pipeline})

    def begin(self):
        if self.db_instance != "mongodb":
            raise TransactionError(f"Transactions are not supported for db_instance={self.db_instance}")
        try:
            self._ensure_mongo()
            if self._active_session() is not None:
                raise TransactionError("Transaction already active on this thread")
            session = self._client.start_session()
            session.start_transaction()
            self._local.session = session
            return session
        except TransactionError:
            raise
        except Exception as exc:
            self._log_error("transaction.error", "Failed to start Mongo transaction", exc)
            if "session" in locals():
                session.end_session()
            raise TransactionError("Transaction start failed") from exc

    def commit(self) -> None:
        session = self._active_session()
        if session is None:
            raise TransactionError("No active transaction to commit")
        try:
            session.commit_transaction()
        except Exception as exc:
            self._log_error("transaction.error", "Mongo commit failed", exc)
            raise TransactionError("Commit failed") from exc
        finally:
            session.end_session()
            self._local.session = None

    def rollback(self) -> None:
        session = self._active_session()
        if session is None:
            raise TransactionError("No active transaction to rollback")
        try:
            session.abort_transaction()
        except Exception as exc:
            self._log_error("transaction.error", "Mongo rollback failed", exc)
            raise TransactionError("Rollback failed") from exc
        finally:
            session.end_session()
            self._local.session = None

    @contextmanager
    def transaction(self):
        self.begin()
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise

    def _active_session(self):
        return getattr(self._local, "session", None)

    def _normalize_where(self, where: Mapping[str, Any] | str | None) -> dict[str, Any]:
        if where is None:
            return {}
        if isinstance(where, Mapping):
            return self._sanitize_where_mapping(where)
        if isinstance(where, str):
            return self._convert_condition(where)
        raise QueryError("where must be mapping, string, or None")

    def _sanitize_where_mapping(self, where: Mapping[str, Any]) -> dict[str, Any]:
        if not where:
            return {}
        sanitized: dict[str, Any] = {}
        for key, value in where.items():
            field = str(key).strip()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field):
                raise QueryError("where mapping contains invalid field identifier")
            if isinstance(value, Mapping):
                raise QueryError("Mongo operator expressions are not allowed in where mappings")
            if isinstance(value, list):
                raise QueryError("List values are not allowed in where mappings")
            sanitized[field] = value
        return sanitized

    def _parse_order_by(self, order_by: str):
        text = order_by.strip()
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)(?:\s+(ASC|DESC))?", text, flags=re.IGNORECASE)
        if not match:
            raise QueryError("Invalid order_by clause")
        field = match.group(1)
        direction = (match.group(2) or "ASC").upper()
        return [(field, 1 if direction == "ASC" else -1)]

    def _normalize_group_fields(self, group_by: str | list[str] | tuple[str, ...] | None) -> list[str]:
        if group_by is None:
            return []
        if isinstance(group_by, str):
            fields = [group_by.strip()]
        elif isinstance(group_by, (list, tuple)):
            fields = [str(item).strip() for item in group_by]
        else:
            raise QueryError("group_by must be a string, list, tuple, or None")
        normalized: list[str] = []
        for field in fields:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field):
                raise QueryError(f"Invalid field identifier: {field!r}")
            normalized.append(field)
        return normalized

    def _normalize_aggregate_metric(self, alias: str, metric: Any) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias):
            raise QueryError(f"Invalid aggregate alias: {alias!r}")
        if isinstance(metric, str):
            match = re.fullmatch(
                r"\s*(count|sum|avg|min|max)\s*\(\s*(\*|[A-Za-z_][A-Za-z0-9_]*)\s*\)\s*",
                metric,
                flags=re.IGNORECASE,
            )
            if not match:
                raise QueryError("Invalid aggregate metric format; expected e.g. count(*), sum(field)")
            op = match.group(1).lower()
            field = match.group(2)
        elif isinstance(metric, Mapping):
            op = str(metric.get("op", "")).strip().lower()
            field = str(metric.get("field", "*")).strip()
            if op not in {"count", "sum", "avg", "min", "max"}:
                raise QueryError(f"Unsupported aggregate op: {op}")
            if field != "*" and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field):
                raise QueryError(f"Invalid aggregate field: {field!r}")
        else:
            raise QueryError("metrics values must be strings like 'count(*)' or mappings")
        if op == "count":
            if field == "*":
                return {alias: {"$sum": 1}}
            return {alias: {"$sum": {"$cond": [{"$ne": [f"${field}", None]}, 1, 0]}}}
        if field == "*":
            raise QueryError(f"{op.upper()}(*) is not supported; specify a field")
        mongo_op = {"sum": "$sum", "avg": "$avg", "min": "$min", "max": "$max"}[op]
        return {alias: {mongo_op: f"${field}"}}

    def _build_aggregate_pipeline(
        self,
        *,
        group_by: str | list[str] | tuple[str, ...] | None,
        metrics: Mapping[str, Any] | None,
        where: Mapping[str, Any] | str | None,
        having: Mapping[str, Any] | str | None,
        order_by: str | None,
        limit: int | None,
    ) -> list[Mapping[str, Any]]:
        pipeline: list[Mapping[str, Any]] = []
        native_where = self._normalize_where(where)
        if native_where:
            pipeline.append({"$match": native_where})

        group_fields = self._normalize_group_fields(group_by)
        metric_map: dict[str, Any] = {}
        for alias, metric in (metrics or {}).items():
            metric_map.update(self._normalize_aggregate_metric(alias, metric))

        if group_fields or metric_map:
            group_id: Any = None
            if group_fields:
                group_id = {field: f"${field}" for field in group_fields}
            group_stage = {"_id": group_id}
            group_stage.update(metric_map)
            pipeline.append({"$group": group_stage})
            if having is not None:
                pipeline.append({"$match": self._normalize_where(having)})
            if group_fields:
                project_stage: dict[str, Any] = {"_id": 0}
                for field in group_fields:
                    project_stage[field] = f"$_id.{field}"
                for alias in metric_map:
                    project_stage[alias] = f"${alias}"
                pipeline.append({"$project": project_stage})
        elif having is not None:
            raise QueryError("having requires aggregate metrics or group_by")

        if order_by:
            sort_pairs = self._parse_order_by(order_by)
            pipeline.append({"$sort": {field: direction for field, direction in sort_pairs}})
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise QueryError("limit must be a positive integer")
            pipeline.append({"$limit": limit})
        if not pipeline:
            raise QueryError("aggregate requires pipeline or aggregate parameters")
        return pipeline

    def _extract_collection_and_condition(self, uql_query):
        match = re.match(r"(FIND|DELETE)\s+(\w+)\s*(?:WHERE\s+(.+))?", uql_query, re.IGNORECASE)
        if match:
            return match.group(2), match.group(3)
        raise QueryError("Invalid UQL query")

    def _convert_condition(self, condition):
        """Convert simple UQL WHERE condition to Mongo-style format."""
        if not condition:
            return {}
        condition = condition.strip()
        if self._DANGEROUS.search(condition):
            raise QueryError("Potential injection pattern detected in where condition")

        if re.search(r"\s+OR\s+", condition, flags=re.IGNORECASE):
            parts = re.split(r"\s+OR\s+", condition, flags=re.IGNORECASE)
            return {"$or": [self._convert_condition(part.strip()) for part in parts]}

        if re.search(r"\s+AND\s+", condition, flags=re.IGNORECASE):
            parts = re.split(r"\s+AND\s+", condition, flags=re.IGNORECASE)
            return {"$and": [self._convert_condition(part.strip()) for part in parts]}

        return self._convert_simple_expression(condition.strip())

    def _convert_simple_expression(self, expr):
        match = re.fullmatch(
            r"([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*(.+)",
            expr,
            flags=re.IGNORECASE,
        )
        if match:
            key, op, raw = match.group(1), match.group(2), match.group(3)
            value = self._cast_value(raw.strip())
            if op == "=":
                return {key: value}
            if op == "!=":
                return {key: {"$ne": value}}
            if op == ">":
                return {key: {"$gt": value}}
            if op == "<":
                return {key: {"$lt": value}}
            if op == ">=":
                return {key: {"$gte": value}}
            if op == "<=":
                return {key: {"$lte": value}}

        if expr.upper().startswith("HAS "):
            key = expr[4:].strip()
            return {key: {"$exists": True}}

        raise QueryError("Unsupported where expression")

    def _parse_key_value_pairs(self, fields):
        result = {}
        for pair in fields.split(","):
            key, val = pair.split(":", 1)
            result[key.strip()] = self._cast_value(val.strip())
        return result

    def _cast_value(self, val):
        if not isinstance(val, str):
            return val
        stripped = val.strip()
        lower = stripped.lower()
        if lower == "true":
            return True
        if lower == "false":
            return False
        if (stripped.startswith("'") and stripped.endswith("'")) or (
            stripped.startswith('"') and stripped.endswith('"')
        ):
            stripped = stripped[1:-1]
            if stripped.startswith("$"):
                raise QueryError(f"Mongo operator expressions are not allowed as values: {stripped!r}")
            return stripped
        if re.fullmatch(r"-?\d+", stripped):
            return int(stripped)
        if re.fullmatch(r"-?\d+\.\d+", stripped):
            return float(stripped)
        if stripped.startswith("$"):
            raise QueryError(f"Mongo operator expressions are not allowed as values: {stripped!r}")
        return stripped

    def _ensure_mongo(self):
        if self._db is not None:
            return

        try:
            timeout_ms = int(self.options.get("connect_timeout_ms", 3000))
            max_pool_size = int(self.options.get("max_pool_size", 100))
            min_pool_size = int(self.options.get("min_pool_size", 0))
            self._client = self._conn_manager.get_client(
                self.url,
                max_pool_size=max_pool_size,
                min_pool_size=min_pool_size,
                connect_timeout_ms=timeout_ms,
                server_selection_timeout_ms=timeout_ms,
            )
            db_name = self.options.get("db_name") or self._extract_db_name(self.url) or "udom"
            self._db = self._client[db_name]
            self._db.command("ping")
        except Exception as exc:
            self._log_error("connection.error", "Mongo connection failed", exc)
            raise ConnectionError(self._PUBLIC_CONNECTION_ERROR) from exc

    def _extract_db_name(self, mongo_url):
        parsed = urlparse(mongo_url)
        path = (parsed.path or "").strip("/")
        return path.split("/")[0] if path else None

    def _serialize_doc(self, doc):
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def _log_error(self, event: str, message: str, exc: Exception) -> None:
        log_event(self._logger, 40, message, event=event, db=self.db_instance)
        log_internal_debug(
            self._logger,
            f"{message} (internal)",
            event=f"{event}.internal",
            db=self.db_instance,
            exc=exc,
        )

    def ping(self) -> Any:
        if self.db_instance == "mongodb":
            self._ensure_mongo()
            return self._run_with_retry(lambda: self._db.command("ping"), operation="ping", entity="-")
        return {"ok": 1, "db_instance": self.db_instance}

    def close(self) -> None:
        if self.db_instance == "mongodb":
            self._conn_manager.close_client(self.url)
        self._client = None
        self._db = None

    def ensure_indexes(self, entity: str, indexes: list[Mapping[str, Any]]) -> Any:
        if self.db_instance != "mongodb":
            return {"status": "noop", "db_instance": self.db_instance}
        self._ensure_mongo()
        if not isinstance(indexes, list):
            raise QueryError("indexes must be a list")
        collection = self._db[entity.lower()]
        created: list[str] = []
        for index in indexes:
            if not isinstance(index, Mapping):
                raise QueryError("each index must be a mapping")
            fields = index.get("fields")
            if not isinstance(fields, list) or not fields:
                raise QueryError("index.fields must be a non-empty list")
            options = dict(index.get("options", {}))
            keys = []
            for item in fields:
                if not isinstance(item, Mapping) or "name" not in item:
                    raise QueryError("each index field must include 'name'")
                name = str(item["name"])
                order = str(item.get("order", "asc")).lower()
                keys.append((name, 1 if order == "asc" else -1))
            created_name = self._run_with_retry(
                lambda: collection.create_index(keys, **options),
                operation="create_index",
                entity=entity,
            )
            created.append(str(created_name))
        return {"created_indexes": created, "count": len(created)}

    def _run_with_retry(self, fn, *, operation: str, entity: str):
        last_exc: Exception | None = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                return fn()
            except Exception as exc:
                last_exc = exc
                if not self._is_transient_mongo_error(exc) or attempt >= self._retry_attempts:
                    raise
                backoff = (self._retry_backoff_ms * attempt) / 1000.0
                log_event(
                    self._logger,
                    30,
                    f"Retrying Mongo operation {operation} attempt={attempt}",
                    event="query.retry",
                    db=self.db_instance,
                    entity=entity,
                )
                time.sleep(backoff)
        raise last_exc if last_exc else QueryError("Unknown retry failure")

    @staticmethod
    def _is_transient_mongo_error(exc: Exception) -> bool:
        name = exc.__class__.__name__.lower()
        if name in {
            "autoreconnect",
            "networktimeout",
            "notprimaryerror",
            "serverselectiontimeouterror",
            "executiontimeout",
        }:
            return True
        message = str(exc).lower()
        return any(
            token in message
            for token in (
                "timed out",
                "connection reset",
                "connection refused",
                "not primary",
                "temporary",
            )
        )
