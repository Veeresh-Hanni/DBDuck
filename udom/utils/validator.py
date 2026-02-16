# udm/utils/validator.py

import re

class UQLValidator:
    """Validates UQL syntax, security, and safe conditions"""

    allowed_actions = ["FIND", "CREATE", "UPDATE", "DELETE"]
    
    def is_valid_syntax(self, uql_query):
        if not isinstance(uql_query, str):
            return False
        
        return any(uql_query.upper().startswith(action) for action in self.allowed_actions)

    def check_for_injection(self, query):
        blacklist = [" DROP ", " TRUNCATE ", "--", ";", "/*", "*/"]
        return not any(bad in query.upper() for bad in blacklist)

    def validate(self, uql_query):
        if not self.is_valid_syntax(uql_query):
            return {"valid": False, "error": "Invalid UQL syntax"}

        if not self.check_for_injection(uql_query):
            return {"valid": False, "error": "Potential injection risk"}

        return {"valid": True, "message": "UQL is valid"}
