import re
from typing import Any, Mapping

from ...core.exceptions import QueryError
from .base_adapter import BaseAdapter


class GraphAdapter(BaseAdapter):
    """Adapter for Graph databases (Neo4j / Cypher Query Language)."""

    def __init__(self, db_instance="neo4j", url=None, **options):
        self.db_instance = db_instance
        self.url = url
        self.options = options

    def run_native(self, query: Any, params: Mapping[str, Any] | None = None):
        if params:
            raise QueryError("GraphAdapter does not support params argument")
        return query

    def convert_uql(self, uql_query: str):
        uql_query = uql_query.strip()

        if uql_query.upper().startswith("FIND"):
            label, condition = self._extract_label_and_condition(uql_query)
            where_clause = self._convert_conditions(condition)
            return f"MATCH (n:{label}) {where_clause} RETURN n;"

        if uql_query.upper().startswith("CREATE"):
            label, properties = self._extract_label_and_body(uql_query)
            props = self._convert_create_properties(properties)
            return f"CREATE (n:{label} {props}) RETURN n;"

        if uql_query.upper().startswith("DELETE"):
            label, condition = self._extract_label_and_condition(uql_query)
            where_clause = self._convert_conditions(condition)
            return f"MATCH (n:{label}) {where_clause} DELETE n;"

        raise QueryError("Unsupported or invalid UQL syntax")

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        body = ", ".join([f"{k}: {self._to_uql_value(v)}" for k, v in data.items()])
        return self.run_native(self.convert_uql(f"CREATE {entity} " + "{" + body + "}"))

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        return [self.create(entity, row) for row in rows]

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        if order_by is not None or limit is not None:
            raise QueryError("GraphAdapter.find currently supports only entity + where")
        where_clause = self._normalize_where(where)
        query = f"FIND {entity}" + (f" WHERE {where_clause}" if where_clause else "")
        return self.run_native(self.convert_uql(query))

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        where_clause = self._normalize_where(where)
        if not where_clause:
            raise QueryError("delete requires a non-empty where condition")
        return self.run_native(self.convert_uql(f"DELETE {entity} WHERE {where_clause}"))

    @staticmethod
    def _to_uql_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "\\'")
        return f"'{escaped}'"

    def _normalize_where(self, where: Mapping[str, Any] | str | None) -> str:
        if where is None:
            return ""
        if isinstance(where, str):
            return where.strip()
        if isinstance(where, Mapping):
            parts = [f"{k} = {self._to_uql_value(v)}" for k, v in where.items()]
            return " AND ".join(parts)
        raise QueryError("where must be mapping, string, or None")

    def _extract_label_and_condition(self, uql_query):
        match = re.match(r"(FIND|DELETE)\s+(\w+)\s*(?:WHERE\s+(.+))?", uql_query, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid UQL query")
        label = match.group(2)
        condition = match.group(3) if match.group(3) else ""
        return label, condition

    def _extract_label_and_body(self, uql_query):
        match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", uql_query, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid CREATE UQL")
        return match.group(1), match.group(2)

    def _convert_conditions(self, condition):
        if not condition:
            return ""
        cypher_conditions = []
        parts = re.split(r"\s+AND\s+", condition, flags=re.IGNORECASE)
        for part in parts:
            part = part.strip()
            if ">" in part:
                key, val = part.split(">", 1)
                cypher_conditions.append(f"n.{key.strip()} > {val.strip()}")
            elif "<" in part:
                key, val = part.split("<", 1)
                cypher_conditions.append(f"n.{key.strip()} < {val.strip()}")
            elif "=" in part:
                key, val = part.split("=", 1)
                val = val.strip()
                if val.lower() in ["true", "false"]:
                    cypher_conditions.append(f"n.{key.strip()} = {val.lower()}")
                elif val.isdigit():
                    cypher_conditions.append(f"n.{key.strip()} = {val}")
                else:
                    cypher_conditions.append(f'n.{key.strip()} = "{val.strip(chr(39)).strip(chr(34))}"')
            elif part.upper().startswith("HAS "):
                rel = part[4:].strip()
                cypher_conditions.append(f"(n)-[:{rel.upper()}]->()")
        return f"WHERE {' AND '.join(cypher_conditions)}" if cypher_conditions else ""

    def _convert_create_properties(self, fields):
        props = {}
        pairs = fields.split(",")
        for pair in pairs:
            key, val = pair.split(":", 1)
            key, val = key.strip(), val.strip()
            if val.lower() in ["true", "false"]:
                props[key] = val.lower()
            elif val.isdigit():
                props[key] = val
            else:
                normalized = val.strip(chr(39)).strip(chr(34))
                props[key] = f'"{normalized}"'
        return "{" + ", ".join([f"{k}: {v}" for k, v in props.items()]) + "}"
