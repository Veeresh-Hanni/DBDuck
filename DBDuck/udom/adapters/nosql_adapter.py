import re
from typing import Any, Mapping
from urllib.parse import urlparse

from ...core.exceptions import ConnectionError, QueryError
from .base_adapter import BaseAdapter


class NoSQLAdapter(BaseAdapter):
    """Adapter for NoSQL databases (MongoDB-style BSON format)."""

    def __init__(self, db_instance="mongodb", url="mongodb://localhost:27017/udom", **options):
        self.db_instance = db_instance
        self.url = url or "mongodb://localhost:27017/udom"
        self.options = options
        self._client = None
        self._db = None

    def run_native(self, query: Any, params: Mapping[str, Any] | None = None):
        """Execute Mongo operation dicts when db_instance is mongodb."""
        if params:
            raise QueryError("NoSQLAdapter does not support SQL-style params argument")

        if self.db_instance != "mongodb":
            return query

        self._ensure_mongo()

        if isinstance(query, str):
            cmd = query.strip().lower()
            if cmd in {"show dbs", "show databases"}:
                return [db.get("name") for db in self._client.list_databases()]
            if cmd == "ping":
                return self._db.command("ping")
            return {"error": "Unsupported Mongo string command. Use dict operations or 'show dbs'/'ping'."}

        if not isinstance(query, dict):
            return {"error": "Mongo native query must be a dict operation."}

        if "find" in query:
            collection = query["find"]
            where = query.get("where", {})
            cursor = self._db[collection].find(where)
            return [self._serialize_doc(doc) for doc in cursor]

        if "insert" in query:
            collection = query["insert"]
            document = query.get("document", {})
            result = self._db[collection].insert_one(document)
            return {"inserted_id": str(result.inserted_id)}

        if "delete" in query:
            collection = query["delete"]
            where = query.get("where", {})
            result = self._db[collection].delete_many(where)
            return {"deleted_count": result.deleted_count}

        if "update" in query:
            collection = query["update"]
            where = query.get("where", {})
            values = query.get("values", {})
            result = self._db[collection].update_many(where, {"$set": values})
            return {"matched_count": result.matched_count, "modified_count": result.modified_count}

        return {"error": "Unsupported Mongo operation."}

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

        return {"error": "Unsupported or invalid UQL syntax"}

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        if not isinstance(data, Mapping) or not data:
            raise QueryError("data must be a non-empty mapping")
        return self.run_native({"insert": entity.lower(), "document": dict(data)})

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        if not isinstance(rows, list) or not rows:
            raise QueryError("rows must be a non-empty list")
        results = [self.create(entity, row) for row in rows]
        return {"inserted_count": len(results), "results": results}

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        if order_by is not None or limit is not None:
            raise QueryError("NoSQLAdapter.find currently supports only entity + where")
        if where is None:
            native_where = {}
        elif isinstance(where, Mapping):
            native_where = dict(where)
        elif isinstance(where, str):
            native_where = self._convert_condition(where)
        else:
            raise QueryError("where must be mapping, string, or None")
        return self.run_native({"find": entity.lower(), "where": native_where})

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        if isinstance(where, Mapping):
            native_where = dict(where)
        elif isinstance(where, str):
            native_where = self._convert_condition(where)
        else:
            raise QueryError("where must be mapping or string")
        if not native_where:
            raise QueryError("delete requires a non-empty where condition")
        return self.run_native({"delete": entity.lower(), "where": native_where})

    def _extract_collection_and_condition(self, uql_query):
        match = re.match(r"(FIND|DELETE)\s+(\w+)\s*(?:WHERE\s+(.+))?", uql_query, re.IGNORECASE)
        if match:
            return match.group(2), match.group(3)
        return None, None

    def _convert_condition(self, condition):
        """Convert simple UQL WHERE condition to Mongo-style format."""
        if not condition:
            return {}

        if re.search(r"\s+OR\s+", condition, flags=re.IGNORECASE):
            parts = re.split(r"\s+OR\s+", condition, flags=re.IGNORECASE)
            return {"$or": [self._convert_condition(part.strip()) for part in parts]}

        if re.search(r"\s+AND\s+", condition, flags=re.IGNORECASE):
            parts = re.split(r"\s+AND\s+", condition, flags=re.IGNORECASE)
            return {"$and": [self._convert_condition(part.strip()) for part in parts]}

        return self._convert_simple_expression(condition.strip())

    def _convert_simple_expression(self, expr):
        if ">" in expr:
            key, val = expr.split(">", 1)
            return {key.strip(): {"$gt": self._cast_value(val.strip())}}

        if "<" in expr:
            key, val = expr.split("<", 1)
            return {key.strip(): {"$lt": self._cast_value(val.strip())}}

        if "=" in expr:
            key, val = expr.split("=", 1)
            return {key.strip(): self._cast_value(val.strip())}

        if expr.upper().startswith("HAS "):
            key = expr[4:].strip()
            return {key: {"$exists": True}}

        return {"$raw": expr}

    def _parse_key_value_pairs(self, fields):
        result = {}
        for pair in fields.split(","):
            key, val = pair.split(":", 1)
            result[key.strip()] = self._cast_value(val.strip())
        return result

    def _cast_value(self, val):
        if val.lower() == "true":
            return True
        if val.lower() == "false":
            return False
        if val.isdigit():
            return int(val)
        if val.replace(".", "", 1).isdigit():
            return float(val)
        return val.strip('"').strip("'")

    def _ensure_mongo(self):
        if self._db is not None:
            return

        try:
            from pymongo import MongoClient
        except Exception as exc:
            raise ConnectionError("pymongo is required for MongoDB support") from exc

        timeout_ms = int(self.options.get("connect_timeout_ms", 3000))
        self._client = MongoClient(self.url, serverSelectionTimeoutMS=timeout_ms)
        db_name = self.options.get("db_name") or self._extract_db_name(self.url) or "udom"
        self._db = self._client[db_name]
        self._db.command("ping")

    def _extract_db_name(self, mongo_url):
        parsed = urlparse(mongo_url)
        path = (parsed.path or "").strip("/")
        return path.split("/")[0] if path else None

    def _serialize_doc(self, doc):
        if "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc
