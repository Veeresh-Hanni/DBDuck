from .base_adapter import BaseAdapter


class VectorAdapter(BaseAdapter):
    """Adapter stub for vector databases (Qdrant, Pinecone, Weaviate, etc.)."""

    def __init__(self, db_instance="qdrant", url=None, **options):
        self.db_instance = db_instance
        self.url = url
        self.options = options

    def run_native(self, query):
        return {
            "db_type": "vector",
            "db_instance": self.db_instance,
            "url": self.url,
            "native_query": query,
            "note": "Vector adapter is currently a pass-through stub.",
        }

    def convert_uql(self, uql_query):
        return {
            "action": "vector_uql_passthrough",
            "db_instance": self.db_instance,
            "uql": uql_query,
        }
