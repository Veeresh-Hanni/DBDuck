# udm/adapters/nosql_adapter.py

import re
from .base_adapter import BaseAdapter

class NoSQLAdapter(BaseAdapter):
    """Adapter for NoSQL databases (MongoDB-style BSON format)"""
    def __init__(self, db_instance="mongodb", url=None, **options):
        self.db_instance = db_instance
        self.url = url
        self.options = options

    def run_native(self, query):
        """
        Executes native document-based NoSQL query.
        For now, just prints. (Later connect to real DB)
        """
        print(f"Executing NoSQL Query ({self.db_instance}):", query)
        return query

    def convert_uql(self, uql_query):
        """
        Very basic UQL → Mongo-style query conversion.
        
        Supported syntax:
        - FIND User WHERE age > 25 AND active = true
        - DELETE User WHERE id = 10
        - CREATE User {name: "Veeresh", age: 23}

        Output is a dictionary in MongoDB query format.
        """

        uql_query = uql_query.strip()

        # 👉 UQL: FIND
        if uql_query.startswith("FIND"):
            collection, condition = self._extract_collection_and_condition(uql_query)
            return {collection.lower(): self._convert_condition(condition)}

        # 👉 UQL: DELETE
        elif uql_query.startswith("DELETE"):
            collection, condition = self._extract_collection_and_condition(uql_query)
            return {"delete": collection.lower(), "where": self._convert_condition(condition)}

        # 👉 UQL: CREATE
        elif uql_query.startswith("CREATE"):
            match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", uql_query, re.IGNORECASE)
            if match:
                collection = match.group(1).lower()
                fields = match.group(2)
                return {collection: self._parse_key_value_pairs(fields)}

        else:
            return {"error": "Unsupported or invalid UQL syntax"}

    # ---------------------------------
    # 🧠 Helper Methods
    # ---------------------------------

    def _extract_collection_and_condition(self, uql_query):
        match = re.match(r"(FIND|DELETE)\s+(\w+)\s*(?:WHERE\s+(.+))?", uql_query, re.IGNORECASE)
        if match:
            return match.group(2), match.group(3)
        return None, None

    def _convert_condition(self, condition):
        """Convert simple UQL WHERE condition to Mongo-style format."""
        if not condition:
            return {}

        mongo_conditions = {}
        parts = condition.split("AND")
        
        for part in parts:
            part = part.strip()

            # Handle ">" operator
            if ">" in part:
                key, val = part.split(">")
                mongo_conditions[key.strip()] = {"$gt": self._cast_value(val.strip())}

            # Handle "<" operator
            elif "<" in part:
                key, val = part.split("<")
                mongo_conditions[key.strip()] = {"$lt": self._cast_value(val.strip())}

            # Handle "=" operator
            elif "=" in part:
                key, val = part.split("=")
                mongo_conditions[key.strip()] = self._cast_value(val.strip())

            # Handle boolean existence (HAS)
            elif part.startswith("HAS"):
                key = part.replace("HAS", "").strip()
                mongo_conditions[key] = {"$exists": True}

        return mongo_conditions

    def _parse_key_value_pairs(self, fields):
        """Parse Create syntax into MongoDB insert format."""
        result = {}
        pairs = fields.split(",")

        for pair in pairs:
            key, val = pair.split(":")
            result[key.strip()] = self._cast_value(val.strip())

        return result

    def _cast_value(self, val):
        """Auto convert values to int/bool/str."""
        if val.lower() == "true":
            return True
        if val.lower() == "false":
            return False
        if val.isdigit():
            return int(val)
        if val.replace(".", "", 1).isdigit():
            return float(val)
        return val.strip('"').strip("'")
