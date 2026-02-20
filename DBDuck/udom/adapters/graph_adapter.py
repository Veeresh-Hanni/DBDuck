# udm/adapters/graph_adapter.py

import re
from .base_adapter import BaseAdapter

class GraphAdapter(BaseAdapter):
    """Adapter for Graph databases (Neo4j / Cypher Query Language)"""
    def __init__(self, db_instance="neo4j", url=None, **options):
        self.db_instance = db_instance
        self.url = url
        self.options = options

    def run_native(self, query):
        """
        Executes native Cypher queries.
        (Later: Use official Neo4j driver for real execution)
        """
        print(f"Executing Cypher Query ({self.db_instance}):", query)
        return query

    def convert_uql(self, uql_query):
        """
        Convert simple UQL → Cypher format
        
        Supported syntax:
        - FIND User WHERE age > 25 AND active = true
        - FIND User WHERE HAS friends
        - CREATE User {name: "Veeresh", age: 23}
        - DELETE User WHERE inactive = true

        Output is Cypher string.
        """

        uql_query = uql_query.strip()

        # 👉 UQL: FIND
        if uql_query.startswith("FIND"):
            label, condition = self._extract_label_and_condition(uql_query)
            where_clause = self._convert_conditions(condition)
            return f"MATCH (n:{label}) {where_clause} RETURN n;"

        # 👉 UQL: CREATE
        elif uql_query.startswith("CREATE"):
            label, properties = self._extract_label_and_body(uql_query)
            props = self._convert_create_properties(properties)
            return f"CREATE (n:{label} {props}) RETURN n;"

        # 👉 UQL: DELETE
        elif uql_query.startswith("DELETE"):
            label, condition = self._extract_label_and_condition(uql_query)
            where_clause = self._convert_conditions(condition)
            return f"MATCH (n:{label}) {where_clause} DELETE n;"

        else:
            return "/* Unsupported or invalid UQL syntax */"

    # ----------------------------------------------------
    # 🧠 Helper Methods
    # ----------------------------------------------------

    def _extract_label_and_condition(self, uql_query):
        """Extract Node Label and WHERE condition."""
        match = re.match(r"(FIND|DELETE)\s+(\w+)\s*(?:WHERE\s+(.+))?", uql_query, re.IGNORECASE)
        label = match.group(2)
        condition = match.group(3) if match.group(3) else ""
        return label, condition

    def _extract_label_and_body(self, uql_query):
        """Extract Node Label and property body for CREATE"""
        match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", uql_query, re.IGNORECASE)
        return match.group(1), match.group(2)

    def _convert_conditions(self, condition):
        """Convert UQL WHERE condition to Cypher WHERE clause."""
        if not condition:
            return ""

        cypher_conditions = []
        parts = condition.split("AND")

        for part in parts:
            part = part.strip()

            if ">" in part:
                key, val = part.split(">")
                cypher_conditions.append(f"n.{key.strip()} > {val.strip()}")

            elif "<" in part:
                key, val = part.split("<")
                cypher_conditions.append(f"n.{key.strip()} < {val.strip()}")

            elif "=" in part:
                key, val = part.split("=")
                val = val.strip()
                if val.lower() in ["true", "false"]:
                    cypher_conditions.append(f"n.{key.strip()} = {val.lower()}")
                elif val.isdigit():
                    cypher_conditions.append(f"n.{key.strip()} = {val}")
                else:
                    cypher_conditions.append(f'n.{key.strip()} = "{val}"')

            elif part.startswith("HAS"):
                rel = part.replace("HAS", "").strip()
                cypher_conditions.append(f"(n)-[:{rel.upper()}]->()")

        return f"WHERE {' AND '.join(cypher_conditions)}"

    def _convert_create_properties(self, fields):
        """Convert CREATE body to valid Cypher map."""
        props = {}
        pairs = fields.split(",")

        for pair in pairs:
            key, val = pair.split(":")
            key, val = key.strip(), val.strip()

            if val.lower() in ["true", "false"]:
                props[key] = val.lower()
            elif val.isdigit():
                props[key] = val
            else:
                props[key] = f'"{val}"'

        return "{" + ", ".join([f"{k}: {v}" for k, v in props.items()]) + "}"
