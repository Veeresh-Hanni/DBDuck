from .base_adapter import BaseAdapter


class AIAdapter(BaseAdapter):
    """Adapter stub for AI-native backends (LLM APIs, agent stores, etc.)."""

    def __init__(self, db_instance="openai", url=None, **options):
        self.db_instance = db_instance
        self.url = url
        self.options = options

    def run_native(self, query):
        return {
            "db_type": "ai",
            "db_instance": self.db_instance,
            "url": self.url,
            "native_query": query,
            "note": "AI adapter is currently a pass-through stub.",
        }

    def convert_uql(self, uql_query):
        return {
            "action": "ai_uql_passthrough",
            "db_instance": self.db_instance,
            "uql": uql_query,
        }
