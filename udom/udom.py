from udom.models.umodel import UModel
from .adapters.ai_adapter import AIAdapter
from .adapters.graph_adapter import GraphAdapter
from .adapters.nosql_adapter import NoSQLAdapter
from .adapters.sql.mariadb_adapter import MariaDBAdapter
from .adapters.sql.mysql_adapter import MySQLAdapter
from .adapters.sql.postgres_adapter import PostgresAdapter
from .adapters.sql.sqlite_adapter import SQLiteAdapter
from .adapters.sql_adapter import SQLAdapter
from .adapters.vector_adapter import VectorAdapter
from .uql.uql_parser import UQLParser
from .utils.validator import UQLValidator


class UDOM:
    """Universal data object model across SQL, NoSQL, Graph, AI, and Vector backends."""

    _SUPPORTED_DB_TYPES = {"sql", "nosql", "graph", "ai", "vector"}
    _SQL_ENGINES = {
        "sqlite",
        "mysql",
        "postgres",
        "postgresql",
        "mariadb",
        "mssql",
        "sqlserver",
    }
    _NOSQL_ENGINES = {"mongodb", "mongo", "redis", "dynamodb", "firestore", "cassandra"}
    _GRAPH_ENGINES = {"neo4j", "tigergraph", "rdf"}
    _VECTOR_ENGINES = {"qdrant", "pinecone", "weaviate", "milvus", "chroma", "pgvector"}
    _AI_ENGINES = {"openai", "azure-openai", "bedrock", "vertexai", "ollama"}

    def __init__(self, db_type="sql", db_instance=None, server=None, url=None, **options):
        # Backward compatibility: allow old API UDOM(db_type="mysql")
        self.db_type, self.db_instance = self._normalize_config(db_type, db_instance or server)
        self.url = url or self._default_url(self.db_type, self.db_instance)
        self.options = options
        self.parser = UQLParser()
        self.validator = UQLValidator()
        self.adapter = self.get_adapter()

    def _normalize_config(self, db_type, db_instance):
        db_type_value = (db_type or "").lower()
        db_instance_value = (db_instance or "").lower()

        if db_type_value in self._SUPPORTED_DB_TYPES:
            if not db_instance_value:
                db_instance_value = self._default_instance(db_type_value)
            return db_type_value, self._normalize_instance_alias(db_instance_value)

        # Legacy route: first argument is an engine, infer db_type.
        engine = self._normalize_instance_alias(db_type_value)
        if engine in self._SQL_ENGINES:
            return "sql", "postgres" if engine == "postgresql" else engine
        if engine in self._NOSQL_ENGINES:
            return "nosql", "mongodb" if engine == "mongo" else engine
        if engine in self._GRAPH_ENGINES:
            return "graph", engine
        if engine in self._VECTOR_ENGINES:
            return "vector", engine
        if engine in self._AI_ENGINES:
            return "ai", engine

        raise ValueError(
            "Unsupported db_type/db_instance. Example: "
            "UDOM(db_type='sql', db_instance='mysql', url='...')"
        )

    def _normalize_instance_alias(self, db_instance):
        aliases = {
            "postgresql": "postgres",
            "mongo": "mongodb",
            "sqlserver": "mssql",
            "ms": "mssql",
            "ms-sql": "mssql",
        }
        return aliases.get(db_instance, db_instance)

    def _default_instance(self, db_type):
        defaults = {
            "sql": "sqlite",
            "nosql": "mongodb",
            "graph": "neo4j",
            "vector": "qdrant",
            "ai": "openai",
        }
        return defaults[db_type]

    def _default_url(self, db_type, db_instance):
        if db_type != "sql":
            return None

        defaults = {
            "sqlite": "sqlite:///test.db",
            "mysql": "mysql+pymysql://root:password@localhost:3306/udom",
            "postgres": "postgresql+psycopg2://postgres:password@localhost:5432/udom",
            "mariadb": "mariadb+pymysql://root:password@localhost:3306/udom",
            "mssql": "mssql+pyodbc://sa:password@localhost:1433/udom?driver=ODBC+Driver+17+for+SQL+Server",
        }
        return defaults.get(db_instance)

    def get_adapter(self):
        if self.db_type == "sql":
            db_map = {
                "sqlite": SQLiteAdapter,
                "mysql": MySQLAdapter,
                "postgres": PostgresAdapter,
                "mariadb": MariaDBAdapter,
                # Generic SQLAlchemy adapter for MSSQL and any SQL URL SQLAlchemy supports.
                "mssql": SQLAdapter,
            }
            adapter_cls = db_map.get(self.db_instance)
            if not adapter_cls:
                raise ValueError(f"Unsupported SQL db_instance: {self.db_instance}")
            return adapter_cls(url=self.url)

        if self.db_type == "nosql":
            return NoSQLAdapter(db_instance=self.db_instance, url=self.url, **self.options)

        if self.db_type == "graph":
            return GraphAdapter(db_instance=self.db_instance, url=self.url, **self.options)

        if self.db_type == "vector":
            return VectorAdapter(db_instance=self.db_instance, url=self.url, **self.options)

        if self.db_type == "ai":
            return AIAdapter(db_instance=self.db_instance, url=self.url, **self.options)

        raise ValueError(f"Unsupported db_type: {self.db_type}")

    def query(self, query):
        return self.adapter.run_native(query)

    def execute(self, query):
        return self.adapter.run_native(query)

    def uquery(self, uql):
        return self.adapter.convert_uql(uql)

    def uexecute(self, uql):
        valid = self.validator.validate(uql)
        if not valid["valid"]:
            return valid
        native_query = self.adapter.convert_uql(uql)
        return self.adapter.run_native(native_query)

    @staticmethod
    def _to_uql_value(value):
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        return f'"{value}"'

    def usave(self, model: UModel):
        table = model.get_name()
        fields = model.get_fields()
        data = {f: getattr(model, f) for f in fields if hasattr(model, f)}
        body = ", ".join([f"{k}: {self._to_uql_value(v)}" for k, v in data.items()])
        uql = f"CREATE {table} " + "{" + body + "}"
        return self.uexecute(uql)

    def ufind(self, model: UModel, where=None):
        table = model.get_name()
        uql = f"FIND {table} WHERE {where}" if where else f"FIND {table}"
        return self.uexecute(uql)

    def udelete(self, model: UModel, where):
        table = model.get_name()
        uql = f"DELETE {table} WHERE {where}"
        return self.uexecute(uql)
