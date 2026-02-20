import re
from urllib.parse import urlparse

from .base_adapter import BaseAdapter


class NoSQLAdapter(BaseAdapter):
    """Adapter for NoSQL databases (MongoDB-style BSON format)."""

    def __init__(self, db_instance="mongodb", url="mongodb://localhost:27017/udom", **options):
        self.db_instance = db_instance
        self.url = url or "mongodb://localhost:27017/udom"
        self.options = options
        self._client = None
        self._db = None

    def run_native(self, query):
        """Execute Mongo operation dicts when db_instance is mongodb."""
        if self.db_instance != "mongodb":
            print(f"Executing NoSQL Query ({self.db_instance}):", query)
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

    def convert_uql(self, uql_query):
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
            raise RuntimeError("pymongo is required for MongoDB support") from exc

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
