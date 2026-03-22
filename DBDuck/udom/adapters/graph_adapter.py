"""Neo4j-capable graph adapter with parameterized Cypher generation."""

from __future__ import annotations

import re
from typing import Any, Mapping

from ...core.exceptions import ConnectionError, QueryError
from ...utils.logger import get_logger, log_event, log_internal_debug
from .base_adapter import BaseAdapter


class GraphAdapter(BaseAdapter):
    """Adapter for graph databases using Cypher, with Neo4j as the primary backend."""

    IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(self, db_instance: str = "neo4j", url: str | None = None, **options: Any) -> None:
        self.db_instance = db_instance
        self.url = url
        self.options = options
        self._logger = get_logger(options.get("log_level"))
        self._driver = options.get("driver")
        self._database = options.get("database")

    def _get_driver(self):
        if self._driver is not None:
            return self._driver
        if self.db_instance != "neo4j":
            return None
        if not self.url:
            return None
        try:
            from neo4j import GraphDatabase
        except Exception:
            return None
        auth = self.options.get("auth")
        if auth is None:
            user = self.options.get("user")
            password = self.options.get("password")
            if user is not None and password is not None:
                auth = (user, password)
        try:
            self._driver = GraphDatabase.driver(self.url, auth=auth)
        except Exception as exc:
            raise ConnectionError("Database connection failed") from exc
        return self._driver

    def run_native(self, query: Any, params: Mapping[str, Any] | None = None):
        if not isinstance(query, str) or not query.strip():
            raise QueryError("Graph query must be a non-empty string")
        bound = dict(params or {})
        driver = self._get_driver()
        if driver is None:
            return {"query": query, "params": bound}
        try:
            log_event(self._logger, 20, "Executing graph query", event="query.execute", db=self.db_instance)
            with driver.session(database=self._database) as session:
                result = session.run(query, bound)
                rows = [record.data() for record in result]
                if rows:
                    return rows
                summary = result.consume()
                counters = summary.counters if summary is not None else None
                return {
                    "nodes_created": getattr(counters, "nodes_created", 0) if counters is not None else 0,
                    "nodes_deleted": getattr(counters, "nodes_deleted", 0) if counters is not None else 0,
                    "relationships_created": getattr(counters, "relationships_created", 0) if counters is not None else 0,
                    "relationships_deleted": getattr(counters, "relationships_deleted", 0) if counters is not None else 0,
                    "properties_set": getattr(counters, "properties_set", 0) if counters is not None else 0,
                }
        except QueryError:
            raise
        except Exception as exc:
            log_event(self._logger, 40, "Graph query failed", event="query.error", db=self.db_instance)
            log_internal_debug(
                self._logger,
                "Internal graph execution failure",
                event="query.error.internal",
                db=self.db_instance,
                exc=exc,
            )
            raise QueryError("Database execution failed") from exc

    def convert_uql(self, uql_query: str):
        text = uql_query.strip()
        upper = text.upper()
        if upper.startswith("FIND"):
            label, condition = self._extract_label_and_condition(text)
            where_clause, params = self._convert_conditions(condition)
            query = f"MATCH (n:{label})"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += " RETURN n;"
            return query, params
        if upper.startswith("CREATE"):
            label, properties = self._extract_label_and_body(text)
            props, params = self._convert_create_properties(properties)
            return f"CREATE (n:{label}) SET n += {props} RETURN n;", params
        if upper.startswith("DELETE"):
            label, condition = self._extract_label_and_condition(text)
            where_clause, params = self._convert_conditions(condition)
            query = f"MATCH (n:{label})"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += " DETACH DELETE n;"
            return query, params
        raise QueryError("Unsupported or invalid UQL syntax")

    def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        label = self._validate_identifier(entity, kind="graph label")
        props, params = self._build_property_map(data, prefix="props")
        return self.run_native(f"CREATE (n:{label}) SET n += {props} RETURN n;", params)

    def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        if not isinstance(rows, list) or not rows:
            raise QueryError("create_many requires a non-empty list")
        return [self.create(entity, row) for row in rows]

    def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        label = self._validate_identifier(entity, kind="graph label")
        where_clause, params = self._normalize_where(where)
        query = f"MATCH (n:{label})"
        if where_clause:
            query += f" WHERE {where_clause}"
        if order_by:
            field, direction = self._parse_order_by(order_by)
            query += f" RETURN n ORDER BY n.{field} {direction}"
        else:
            query += " RETURN n"
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise QueryError("limit must be a positive integer")
            query += " LIMIT $limit_value"
            params["limit_value"] = limit
        query += ";"
        return self.run_native(query, params)

    def update(self, entity: str, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        label = self._validate_identifier(entity, kind="graph label")
        if not isinstance(data, Mapping) or not data:
            raise QueryError("update data must be non-empty")
        where_clause, where_params = self._normalize_where(where)
        if not where_clause:
            raise QueryError("update requires where")
        props, set_params = self._build_property_map(data, prefix="set")
        params = dict(where_params)
        params.update(set_params)
        query = f"MATCH (n:{label}) WHERE {where_clause} SET n += {props} RETURN n;"
        return self.run_native(query, params)

    def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        label = self._validate_identifier(entity, kind="graph label")
        where_clause, params = self._normalize_where(where)
        if not where_clause:
            raise QueryError("delete requires a non-empty where condition")
        return self.run_native(f"MATCH (n:{label}) WHERE {where_clause} DETACH DELETE n;", params)

    def count(self, entity: str, where: Mapping[str, Any] | str | None = None) -> int:
        label = self._validate_identifier(entity, kind="graph label")
        where_clause, params = self._normalize_where(where)
        query = f"MATCH (n:{label})"
        if where_clause:
            query += f" WHERE {where_clause}"
        query += " RETURN count(n) AS total;"
        rows = self.run_native(query, params)
        if isinstance(rows, dict):
            return int(rows.get("total", 0))
        if not rows:
            return 0
        row = rows[0]
        if isinstance(row, Mapping):
            return int(row.get("total", 0))
        return 0

    def create_relationship(
        self,
        from_label: str,
        from_id: Any,
        rel_type: str,
        to_label: str,
        to_id: Any,
        props: Mapping[str, Any] | None = None,
    ) -> Any:
        source_label = self._validate_identifier(from_label, kind="graph label")
        target_label = self._validate_identifier(to_label, kind="graph label")
        relation_type = self._validate_identifier(rel_type, kind="relationship type")
        prop_map, prop_params = self._build_property_map(props or {}, prefix="rel")
        params = {"from_id": from_id, "to_id": to_id, **prop_params}
        query = (
            f"MATCH (a:{source_label} {{id: $from_id}}), (b:{target_label} {{id: $to_id}}) "
            f"CREATE (a)-[r:{relation_type}]->(b) "
        )
        if props:
            query += f"SET r += {prop_map} "
        query += "RETURN r;"
        return self.run_native(query, params)

    def find_related(
        self,
        entity: str,
        id: Any,
        rel_type: str,
        direction: str = "out",
        target_label: str | None = None,
    ) -> Any:
        label = self._validate_identifier(entity, kind="graph label")
        relation_type = self._validate_identifier(rel_type, kind="relationship type")
        direction_value = direction.lower().strip()
        if direction_value not in {"out", "in", "both"}:
            raise QueryError("direction must be one of: out, in, both")
        target = self._validate_identifier(target_label, kind="graph label") if target_label else None
        if direction_value == "out":
            rel_pattern = f"-[r:{relation_type}]->"
        elif direction_value == "in":
            rel_pattern = f"<-[r:{relation_type}]-"
        else:
            rel_pattern = f"-[r:{relation_type}]-"
        target_clause = f":{target}" if target else ""
        query = f"MATCH (n:{label} {{id: $node_id}}){rel_pattern}(m{target_clause}) RETURN m;"
        return self.run_native(query, {"node_id": id})

    def shortest_path(self, from_label: str, from_id: Any, to_label: str, to_id: Any) -> Any:
        source_label = self._validate_identifier(from_label, kind="graph label")
        target_label = self._validate_identifier(to_label, kind="graph label")
        query = (
            f"MATCH path = shortestPath((a:{source_label} {{id: $from_id}})-[*]-(b:{target_label} {{id: $to_id}})) "
            f"RETURN path;"
        )
        return self.run_native(query, {"from_id": from_id, "to_id": to_id})

    def _normalize_where(self, where: Mapping[str, Any] | str | None) -> tuple[str, dict[str, Any]]:
        if where is None:
            return "", {}
        if isinstance(where, str):
            return self._convert_conditions(where.strip())
        if isinstance(where, Mapping):
            if not where:
                return "", {}
            parts = []
            params: dict[str, Any] = {}
            for index, (key, value) in enumerate(where.items()):
                field = self._validate_identifier(str(key), kind="graph field")
                pname = f"w_{index}"
                parts.append(f"n.{field} = ${pname}")
                params[pname] = value
            return " AND ".join(parts), params
        raise QueryError("where must be mapping, string, or None")

    def _extract_label_and_condition(self, uql_query: str) -> tuple[str, str]:
        match = re.match(r"(FIND|DELETE)\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:WHERE\s+(.+))?", uql_query, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid UQL query")
        label = self._validate_identifier(match.group(2), kind="graph label")
        condition = match.group(3) if match.group(3) else ""
        return label, condition

    def _extract_label_and_body(self, uql_query: str) -> tuple[str, str]:
        match = re.match(r"CREATE\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{(.+)\}", uql_query, re.IGNORECASE)
        if not match:
            raise QueryError("Invalid CREATE UQL")
        label = self._validate_identifier(match.group(1), kind="graph label")
        return label, match.group(2)

    def _convert_conditions(self, condition: str) -> tuple[str, dict[str, Any]]:
        if not condition:
            return "", {}
        tokens = re.split(r"\s+(AND|OR)\s+", condition, flags=re.IGNORECASE)
        cypher_conditions: list[str] = []
        params: dict[str, Any] = {}
        index = 0
        for token in tokens:
            part = token.strip()
            if not part:
                continue
            connector = part.upper()
            if connector in {"AND", "OR"}:
                cypher_conditions.append(connector)
                continue
            match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*(.+)", part)
            if not match:
                raise QueryError("Unsupported graph where expression")
            key = self._validate_identifier(match.group(1).strip(), kind="graph field")
            operator = match.group(2)
            pname = f"w_{index}"
            params[pname] = self._parse_literal_value(match.group(3).strip())
            cypher_conditions.append(f"n.{key} {operator} ${pname}")
            index += 1
        return " ".join(cypher_conditions), params

    def _convert_create_properties(self, fields: str) -> tuple[str, dict[str, Any]]:
        data: dict[str, Any] = {}
        for pair in fields.split(","):
            if ":" not in pair:
                raise QueryError("Invalid CREATE UQL payload")
            key, value = pair.split(":", 1)
            field_name = self._validate_identifier(key.strip(), kind="graph field")
            data[field_name] = self._parse_literal_value(value.strip())
        return self._build_property_map(data, prefix="p")

    def _build_property_map(self, data: Mapping[str, Any], *, prefix: str) -> tuple[str, dict[str, Any]]:
        if not isinstance(data, Mapping):
            raise QueryError("properties must be a mapping")
        if not data:
            return "{}", {}
        parts: list[str] = []
        params: dict[str, Any] = {}
        for index, (key, value) in enumerate(data.items()):
            field_name = self._validate_identifier(str(key), kind="graph field")
            pname = f"{prefix}_{index}"
            parts.append(f"{field_name}: ${pname}")
            params[pname] = value
        return "{" + ", ".join(parts) + "}", params

    def _parse_order_by(self, order_by: str) -> tuple[str, str]:
        text = order_by.strip()
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)(?:\s+(ASC|DESC))?", text, flags=re.IGNORECASE)
        if not match:
            raise QueryError("Invalid order_by clause")
        field = self._validate_identifier(match.group(1), kind="graph field")
        direction = (match.group(2) or "ASC").upper()
        return field, direction

    @classmethod
    def _validate_identifier(cls, value: str, *, kind: str) -> str:
        if not isinstance(value, str) or not cls.IDENTIFIER_RE.fullmatch(value):
            raise QueryError(f"Invalid {kind}: {value!r}")
        return value

    @staticmethod
    def _parse_literal_value(raw: str) -> Any:
        value = raw.strip()
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            return value[1:-1]
        lower = value.lower()
        if lower == "true":
            return True
        if lower == "false":
            return False
        if re.fullmatch(r"-?\d+", value):
            return int(value)
        if re.fullmatch(r"-?\d+(?:\.\d+)?", value):
            return float(value)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise QueryError("String values in graph conditions must be quoted")
        return value

    def ping(self) -> Any:
        result = self.run_native("RETURN 1 AS ok;", {})
        if isinstance(result, list) and result:
            return result[0]
        return {"ok": 1, "db_type": "graph", "db_instance": self.db_instance}

    def close(self) -> None:
        driver = self._driver
        if driver is not None:
            try:
                driver.close()
            except Exception as exc:
                log_internal_debug(
                    self._logger,
                    "Graph driver close failed",
                    event="connection.close.internal",
                    db=self.db_instance,
                    exc=exc,
                )
        self._driver = None

