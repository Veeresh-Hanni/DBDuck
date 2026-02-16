# udm/uql/uql_parser.py

import re

class UQLParser:
    """Basic UQL Parser that converts UQL text into structured Python dict"""

    def parse(self, uql_query):
        uql_query = uql_query.strip()

        if uql_query.upper().startswith("FIND"):
            return self._parse_find(uql_query)

        elif uql_query.upper().startswith("CREATE"):
            return self._parse_create(uql_query)

        elif uql_query.upper().startswith("DELETE"):
            return self._parse_delete(uql_query)

        elif uql_query.upper().startswith("UPDATE"):
            return self._parse_update(uql_query)

        else:
            return {"error": "Invalid or unsupported UQL command"}

    # ----------------------------------------------------
    # 🧠 Parse FIND
    # ----------------------------------------------------
    def _parse_find(self, query):
        match = re.match(r"FIND\s+(\w+)(?:\s+WHERE\s+(.+))?", query, re.IGNORECASE)
        return {
            "action": "FIND",
            "entity": match.group(1),
            "condition": match.group(2) if match.group(2) else None
        }

    # ----------------------------------------------------
    # 🧠 Parse CREATE
    # ----------------------------------------------------
    def _parse_create(self, query):
        match = re.match(r"CREATE\s+(\w+)\s*\{(.+)\}", query, re.IGNORECASE)
        fields = match.group(2)
        field_data = self._parse_key_value_pairs(fields)

        return {
            "action": "CREATE",
            "entity": match.group(1),
            "fields": field_data
        }

    # ----------------------------------------------------
    # 🧠 Parse DELETE
    # ----------------------------------------------------
    def _parse_delete(self, query):
        match = re.match(r"DELETE\s+(\w+)(?:\s+WHERE\s+(.+))?", query, re.IGNORECASE)
        return {
            "action": "DELETE",
            "entity": match.group(1),
            "condition": match.group(2) if match.group(2) else None
        }

    # ----------------------------------------------------
    # 🧠 Parse UPDATE
    # ----------------------------------------------------
    def _parse_update(self, query):
        match = re.match(r"UPDATE\s+(\w+)\s+SET\s+(.+)\s+WHERE\s+(.+)", query, re.IGNORECASE)
        fields = self._parse_key_value_pairs(match.group(2))
        return {
            "action": "UPDATE",
            "entity": match.group(1),
            "fields": fields,
            "condition": match.group(3)
        }

    def _parse_key_value_pairs(self, text):
        data = {}
        pairs = text.split(",")
        for pair in pairs:
            key, val = pair.split(":")
            data[key.strip()] = self._cast_value(val.strip())
        return data

    def _cast_value(self, val):
        if val.lower() == "true": return True
        if val.lower() == "false": return False
        if val.isdigit(): return int(val)
        return val.strip('"').strip("'")