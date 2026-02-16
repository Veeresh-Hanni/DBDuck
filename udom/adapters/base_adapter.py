# udm/adapters/base_adapter.py

class BaseAdapter:
    """Interface for all adapters (SQL, NoSQL, Graph)"""

    def run_native(self, query):
        raise NotImplementedError

    def convert_uql(self, uql_query):
        raise NotImplementedError
