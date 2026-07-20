"""Universal model base for SQL and NoSQL backends."""

from __future__ import annotations

import json
import re
import types
from datetime import date, datetime, time
from typing import Any, Mapping, TypeVar, Union, get_args, get_origin, get_type_hints

from sqlalchemy import inspect as sa_inspect
from sqlalchemy import text
from sqlalchemy.schema import CreateColumn

from ...core import SensitiveFieldProtector
from ...core.exceptions import QueryError

TModel = TypeVar("TModel", bound="UModel")


class ModelFieldReference:
    """Class-level field reference for annotation-only UModel fields."""

    def __init__(self, name: str | None = None) -> None:
        self.name = name

    def __set_name__(self, owner, name: str) -> None:
        if self.name is None:
            self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if self.name in instance.__dict__:
            return instance.__dict__[self.name]
        raise AttributeError(self.name or "field")

    def __set__(self, instance, value: Any) -> None:
        instance.__dict__[self.name] = value

    def __str__(self) -> str:
        return str(self.name or "")

    def __repr__(self) -> str:
        return str(self)

    def __neg__(self) -> str:
        return f"-{self}"


class UModel:
    """Base class for all data models in UDOM.

    Works with both SQL and NoSQL adapters through the same UDOM interface.
    """

    _udom = None
    __strict__ = True
    __indexes__: list[Mapping[str, Any]] = []
    __sensitive_fields__: list[str] | tuple[str, ...] | None = None
    __schema_migrations_table__ = "dbduck_schema_migrations"

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for field_name in getattr(cls, "__annotations__", {}):
            if field_name.startswith("_") or hasattr(cls, field_name):
                continue
            descriptor = ModelFieldReference(field_name)
            descriptor.__set_name__(cls, field_name)
            setattr(cls, field_name, descriptor)

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    @classmethod
    def bind(cls, db) -> type["UModel"]:
        """Bind a UDOM instance to this model class."""
        cls._udom = db
        return cls

    def using(self, db) -> "UModel":
        """Bind a UDOM instance to this model object only."""
        self._udom = db
        return self

    @classmethod
    def _all_annotations(cls) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for base in reversed(cls.mro()):
            if base is object:
                continue
            hints = get_type_hints(base, include_extras=False)
            merged.update(hints)
        merged.pop("_udom", None)
        # Internal/class configuration fields are not model payload columns.
        return {k: v for k, v in merged.items() if not (k.startswith("__") and k.endswith("__"))}

    @classmethod
    def get_fields(cls) -> dict[str, Any]:
        """Return model field definitions from type annotations."""
        return cls._all_annotations()

    @classmethod
    def get_name(cls) -> str:
        """Return entity/table/collection name."""
        for attr in ("__entity__", "__table__", "__collection__"):
            value = getattr(cls, attr, None)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return cls.__name__


    def to_dict(
            self,
            *,
            include_none: bool = False,
            only_declared: bool = True,
            include_sensitive: bool = False,
        ) -> dict[str, Any]:
        """Serialize model values to dictionary payload.

        Args:
            include_sensitive: If False (default), fields listed in
                __sensitive_fields__ (e.g. password) are excluded from
                the output. Set True only for trusted internal use.
        """
        if only_declared:
            names = self.get_fields().keys()
            data = {name: getattr(self, name) for name in names if hasattr(self, name)}
        else:
            data = dict(self.__dict__)

        loaded_fields = getattr(self, "_udom_loaded_fields", None)
        if loaded_fields is not None:
            data = {
                key: getattr(self, key)
                for key in loaded_fields
                if hasattr(self, key)
            }

        if not include_sensitive:
            sensitive = set(getattr(self.__class__, "__sensitive_fields__", []) or [])
            data = {k: v for k, v in data.items() if k not in sensitive}

        if not include_none:
            data = {k: v for k, v in data.items() if v is not None}
        return data


    @classmethod
    def from_dict(cls: type[TModel], payload: Mapping[str, Any], *, partial: bool = False) -> TModel:
        """Build a model instance from a mapping payload.

        ``partial`` is intended for projected query results, where fields not
        present in the payload were deliberately omitted by ``select()``.
        """
        if not isinstance(payload, Mapping):
            raise QueryError("from_dict payload must be a mapping")
        model = cls(**dict(payload))
        if partial:
            # Declarative model constructors may populate defaults for columns
            # that were not selected. Preserve the projection boundary when
            # the model is later serialized with to_dict().
            model._udom_loaded_fields = frozenset(payload)
        if model.__strict__:
            model.validate(only_fields=set(payload) if partial else None)
        return model

    def validate(self, *, only_fields: set[str] | None = None) -> None:
        """Basic model validation from declared fields.

        Args:
            only_fields: If provided, only these fields are validated as
                required (used when the instance was hydrated from a
                partial/projected row via .select()). If None, all
                declared fields are validated as normal.
        """
        fields = self.get_fields()
        if not fields:
            if not self.__dict__:
                raise QueryError("Model has no declared fields and no payload values")
            return
        for field, expected_type in fields.items():
            if only_fields is not None and field not in only_fields:
                continue  # field wasn't selected/requested — skip required check
            if not hasattr(self, field):
                if self._is_optional_type(expected_type):
                    setattr(self, field, None)
                    continue
                raise QueryError(f"Missing required model field: {field}")
            if self.__strict__:
                coerced = self._coerce_value(field, getattr(self, field), expected_type)
                setattr(self, field, coerced)

    @classmethod
    def _resolve_db(cls, db=None):
        target = db if db is not None else cls._udom
        if target is None:
            raise QueryError("No UDOM instance bound. Use Model.bind(db) or pass db=...")
        return target

    @classmethod
    def query(cls: type[TModel], db=None) -> "ModelQueryBuilder[TModel]":
        """Return a QueryBuilder for fluent query construction on this model.

        The ModelQueryBuilder wraps QueryBuilder to return typed model instances.

        Args:
            db: Optional UDOM instance (uses bound instance if not provided).

        Returns:
            A ModelQueryBuilder for chaining.

        Example:
            User.bind(db)
            users = User.query().where(active=True).order("name").find()
            user = User.query().where(id=1).first()
            count = User.query().where(role="admin").count()
        """
        resolved = cls._resolve_db(db)
        return ModelQueryBuilder(cls, resolved)

    def _resolve_instance_db(self, db=None):
        target = db if db is not None else getattr(self, "_udom", None) or self.__class__._udom
        if target is None:
            raise QueryError("No UDOM instance bound. Use model.using(db), Model.bind(db), or pass db=...")
        return target

    def save(self, db=None):
        """Insert model payload via bound UDOM backend."""
        self.validate()
        resolved = self._resolve_instance_db(db)
        payload = self._prepare_payload_for_db(self.to_dict(include_sensitive=True), getattr(resolved, "db_type", "sql"))
        if hasattr(resolved, "_create_internal"):
            return resolved._create_internal(self.get_name(), payload, sensitive_fields=self.get_sensitive_fields())
        payload = self._protect_sensitive_fields(payload, resolved)
        return resolved.create(self.get_name(), payload)

    def update(self, data: Mapping[str, Any] | None = None, where: Mapping[str, Any] | str | None = None, db=None):
        """Update records for this model."""
        resolved = self._resolve_instance_db(db)
        payload = dict(data) if data is not None else self.to_dict(include_sensitive=True)
        if where is None:
            inferred = {k: getattr(self, k) for k in ("id", "_id") if hasattr(self, k)}
            if not inferred:
                raise QueryError("update requires where, id, or _id")
            where = inferred
        normalized = self._prepare_payload_for_db(payload, getattr(resolved, "db_type", "sql"))
        if hasattr(resolved, "_update_internal"):
            return resolved._update_internal(self.get_name(), normalized, where, sensitive_fields=self.get_sensitive_fields())
        normalized = self._protect_sensitive_fields(normalized, resolved)
        return resolved.update(self.get_name(), normalized, where=where)

    def delete(self, where: Mapping[str, Any] | str | None = None, db=None):
        """Delete rows/documents using explicit where or inferred key fields."""
        resolved = self._resolve_instance_db(db)
        if where is None:
            candidates = ("id", "_id")
            inferred = {k: getattr(self, k) for k in candidates if hasattr(self, k)}
            if not inferred:
                raise QueryError("delete requires where, id, or _id")
            where = inferred
        return resolved.delete(self.get_name(), where)

    def verify_secret(self, field: str, plain_value: Any) -> bool:
        """Validate plaintext against a stored BCrypt hash on this model field."""
        if not isinstance(field, str) or not field.strip():
            raise QueryError("field must be a non-empty string")
        field_name = field.strip()
        if not hasattr(self, field_name):
            raise QueryError(f"Model has no field: {field_name}")
        return SensitiveFieldProtector.verify_secret(plain_value, getattr(self, field_name))

    @classmethod
    def verify_secret_value(cls, plain_value: Any, stored_hash: Any) -> bool:
        """Validate plaintext against a stored BCrypt hash without a model instance."""
        return SensitiveFieldProtector.verify_secret(plain_value, stored_hash)

    @classmethod
    def find(
        cls: type[TModel],
        where: Mapping[str, Any] | str | None = None,
        *,
        order_by: str | None = None,
        limit: int | None = None,
        db=None,
    ) -> list[TModel]:
        """Query model records and return typed model objects."""
        resolved = cls._resolve_db(db)
        rows = resolved.find(cls.get_name(), where=where, order_by=order_by, limit=limit)
        if isinstance(rows, list):
            return [cls.from_dict(row) if isinstance(row, Mapping) else cls(value=row) for row in rows]
        if isinstance(rows, Mapping) and cls._looks_like_record(rows):
            return [cls.from_dict(rows)]
        return []

    @classmethod
    def find_one(
        cls: type[TModel],
        where: Mapping[str, Any] | str | None = None,
        *,
        order_by: str | None = None,
        db=None,
    ) -> TModel | None:
        """Query first matching model record."""
        results = cls.find(where=where, order_by=order_by, limit=1, db=db)
        return results[0] if results else None

    @classmethod
    def count(cls, where: Mapping[str, Any] | str | None = None, db=None) -> int:
        resolved = cls._resolve_db(db)
        return int(resolved.count(cls.get_name(), where=where))

    @classmethod
    def aggregate(
        cls: type[TModel],
        *,
        group_by: str | list[str] | tuple[str, ...] | None = None,
        metrics: Mapping[str, Any] | None = None,
        where: Mapping[str, Any] | str | None = None,
        having: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        pipeline: list[Mapping[str, Any]] | None = None,
        db=None,
    ) -> list[dict[str, Any]]:
        resolved = cls._resolve_db(db)
        rows = resolved.aggregate(
            cls.get_name(),
            group_by=group_by,
            metrics=metrics,
            where=where,
            having=having,
            order_by=order_by,
            limit=limit,
            pipeline=pipeline,
        )
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, Mapping)]
        if isinstance(rows, Mapping):
            return [dict(rows)]
        return []

    @classmethod
    def find_page(
        cls: type[TModel],
        *,
        page: int = 1,
        page_size: int = 20,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        db=None,
    ) -> dict[str, Any]:
        resolved = cls._resolve_db(db)
        page_data = resolved.find_page(
            cls.get_name(),
            page=page,
            page_size=page_size,
            where=where,
            order_by=order_by,
        )
        page_data["items"] = [cls.from_dict(item) for item in page_data.get("items", []) if isinstance(item, Mapping)]
        return page_data

    @classmethod
    def bulk_create(cls, rows: list["UModel | Mapping[str, Any]"], db=None):
        """Bulk insert model objects or payload dictionaries."""
        resolved = cls._resolve_db(db)
        payloads: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, UModel):
                row.validate()
                payloads.append(row.to_dict(include_sensitive=True))
            elif isinstance(row, Mapping):
                payloads.append(cls.from_dict(dict(row)).to_dict(include_sensitive=True))
            else:
                raise QueryError("bulk_create expects UModel instances or mappings")
        if not payloads:
            raise QueryError("bulk_create requires at least one payload")
        db_type = getattr(resolved, "db_type", "sql")
        normalized = [cls._prepare_payload_for_db(item, db_type) for item in payloads]
        if hasattr(resolved, "_create_many_internal"):
            return resolved._create_many_internal(cls.get_name(), normalized, sensitive_fields=cls.get_sensitive_fields())
        normalized = [cls._protect_sensitive_fields(item, resolved) for item in normalized]
        return resolved.create_many(cls.get_name(), normalized)

    @classmethod
    def ensure_indexes(cls, db=None):
        """Create/ensure indexes declared via __indexes__ for NoSQL backends."""
        resolved = cls._resolve_db(db)
        indexes = getattr(cls, "__indexes__", None)
        if not indexes:
            raise QueryError(f"Model {cls.__name__} has no __indexes__ definitions")
        if hasattr(resolved, "ensure_indexes"):
            return resolved.ensure_indexes(cls.get_name(), list(indexes))
        raise QueryError("Bound UDOM instance does not support ensure_indexes")

    @classmethod
    def create_table(cls, db=None) -> dict[str, Any]:
        resolved = cls._resolve_db(db)
        cls._require_sql_backend(resolved)
        engine = cls._engine_for(resolved)
        cls._ensure_schema_history_table(engine)
        table = cls._declared_table()
        inspector = sa_inspect(engine)
        exists = table.name in inspector.get_table_names()
        if not exists:
            table.create(bind=engine, checkfirst=True)
            cls._record_schema_migration(engine, operation="create_table", details={"table": table.name})
        return {"table": table.name, "created": not exists}

    @classmethod
    def _add_index(cls, engine, index) -> None:
        from sqlalchemy.schema import CreateIndex
        with engine.begin() as conn:
            conn.execute(CreateIndex(index))

    @classmethod
    def migrate(cls, db=None) -> dict[str, Any]:
        resolved = cls._resolve_db(db)
        cls._require_sql_backend(resolved)
        engine = cls._engine_for(resolved)
        cls._ensure_schema_history_table(engine)
        table = cls._declared_table()
        inspector = sa_inspect(engine)
        if table.name not in set(inspector.get_table_names()):
            cls.create_table(db=resolved)
            return {"table": table.name, "created": True, "added_columns": [], "added_indexes": [], "warnings": []}

        existing_columns = {str(col["name"]) for col in inspector.get_columns(table.name)}
        added_columns: list[str] = []
        warnings: list[str] = []
        for column in table.columns:
            if column.name in existing_columns:
                continue
            if column.primary_key or column.unique:
                warnings.append(
                    f"Skipped column '{column.name}': adding primary_key/unique columns automatically is unsafe"
                )
                continue
            if not column.nullable and column.server_default is None:
                warnings.append(f"Skipped column '{column.name}': adding non-nullable columns automatically is unsafe")
                continue
            cls._add_column(engine, table.name, column)
            added_columns.append(column.name)
            cls._record_schema_migration(
                engine,
                operation="add_column",
                details={"table": table.name, "column": column.name},
            )

        # NEW: diff and create missing indexes
        existing_index_names = {str(idx["name"]) for idx in inspector.get_indexes(table.name)}
        added_indexes: list[str] = []
        for index in table.indexes:
            if index.name in existing_index_names:
                continue
            # Skip if it depends on a column we just skipped (unsafe to add)
            index_columns = {col.name for col in index.columns}
            if not index_columns.issubset(existing_columns | set(added_columns)):
                warnings.append(f"Skipped index '{index.name}': references a column not present on the table")
                continue
            cls._add_index(engine, index)
            added_indexes.append(index.name)
            cls._record_schema_migration(
                engine,
                operation="add_index",
                details={"table": table.name, "index": index.name},
            )

        if added_columns or added_indexes:
            cls._invalidate_sql_table_cache(resolved, table.name)
        return {
            "table": table.name,
            "created": False,
            "added_columns": added_columns,
            "added_indexes": added_indexes,
            "warnings": warnings,
            }

    @classmethod
    def ensure_schema(cls, db=None) -> dict[str, Any]:
        return cls.migrate(db=db)

    @classmethod
    def migration_history(cls, db=None) -> list[dict[str, Any]]:
        resolved = cls._resolve_db(db)
        cls._require_sql_backend(resolved)
        engine = cls._engine_for(resolved)
        cls._ensure_schema_history_table(engine)
        _, quoted_history_table = cls._schema_history_table_identifier(engine)
        sql = text(
            f"SELECT model_name, table_name, operation, details, applied_at "  # nosec B608
            f"FROM {quoted_history_table} WHERE table_name = :table_name ORDER BY id ASC"
        )
        with engine.begin() as conn:
            rows = conn.execute(sql, {"table_name": cls.get_name()}).mappings().all()
        history: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            if isinstance(item.get("details"), str) and item["details"]:
                try:
                    item["details"] = json.loads(item["details"])
                except json.JSONDecodeError:
                    pass
            history.append(item)
        return history

    @classmethod
    def _coerce_value(cls, field: str, value: Any, expected_type: Any) -> Any:
        if expected_type is Any:
            return value

        origin = get_origin(expected_type)
        args = get_args(expected_type)

        if origin in (Union, types.UnionType):
            if type(None) in args and value is None:
                return None
            # Try each variant except None.
            union_args = [a for a in args if a is not type(None)]
            errors: list[str] = []
            for variant in union_args:
                try:
                    return cls._coerce_value(field, value, variant)
                except QueryError as exc:
                    errors.append(str(exc))
            raise QueryError(f"Field '{field}' does not match any allowed type: {errors}")

        if origin in (list, tuple):
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        value = parsed
                except json.JSONDecodeError:
                    value = value
            if not isinstance(value, (list, tuple)):
                raise QueryError(f"Field '{field}' must be {origin.__name__}")
            item_type = args[0] if args else Any
            coerced_items = [cls._coerce_value(field, item, item_type) for item in value]
            return coerced_items if origin is list else tuple(coerced_items)

        if origin is dict:
            if not isinstance(value, dict):
                raise QueryError(f"Field '{field}' must be dict")
            key_t = args[0] if len(args) > 0 else Any
            val_t = args[1] if len(args) > 1 else Any
            coerced: dict[Any, Any] = {}
            for k, v in value.items():
                ck = cls._coerce_value(field, k, key_t)
                cv = cls._coerce_value(field, v, val_t)
                coerced[ck] = cv
            return coerced

        if expected_type is bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lower = value.strip().lower()
                if lower in {"true", "1", "yes"}:
                    return True
                if lower in {"false", "0", "no"}:
                    return False
            if isinstance(value, (int, float)) and value in (0, 1):
                return bool(value)
            raise QueryError(f"Field '{field}' must be bool")

        if expected_type is int:
            if isinstance(value, bool):
                raise QueryError(f"Field '{field}' must be int (not bool)")
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.strip().lstrip("-").isdigit():
                return int(value.strip())
            raise QueryError(f"Field '{field}' must be int")

        if expected_type is float:
            if isinstance(value, bool):
                raise QueryError(f"Field '{field}' must be float (not bool)")
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                try:
                    return float(value.strip())
                except ValueError:
                    pass
            raise QueryError(f"Field '{field}' must be float")

        if expected_type is str:
            if isinstance(value, str):
                return value
            if value is None:
                raise QueryError(f"Field '{field}' must be str")
            return str(value)

        if expected_type is datetime:
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                try:
                    return datetime.fromisoformat(value.strip())
                except ValueError as exc:
                    raise QueryError(f"Field '{field}' must be datetime") from exc
            raise QueryError(f"Field '{field}' must be datetime")

        if expected_type is date:
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            if isinstance(value, str):
                try:
                    return date.fromisoformat(value.strip())
                except ValueError as exc:
                    raise QueryError(f"Field '{field}' must be date") from exc
            raise QueryError(f"Field '{field}' must be date")

        if expected_type is time:
            if isinstance(value, time):
                return value
            if isinstance(value, str):
                try:
                    return time.fromisoformat(value.strip())
                except ValueError as exc:
                    raise QueryError(f"Field '{field}' must be time") from exc
            raise QueryError(f"Field '{field}' must be time")

        if isinstance(expected_type, type):
            if isinstance(value, expected_type):
                return value
            try:
                return expected_type(value)
            except Exception as exc:
                raise QueryError(f"Field '{field}' must be {expected_type.__name__}") from exc

        return value

    @classmethod
    def _prepare_payload_for_db(cls, payload: Mapping[str, Any], db_type: str) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for key, value in payload.items():
            normalized[key] = cls._serialize_for_db(value, db_type)
        return normalized

    @classmethod
    def _serialize_for_db(cls, value: Any, db_type: str) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, time):
            return value.isoformat()
        if isinstance(value, list):
            serialized = [cls._serialize_for_db(item, db_type) for item in value]
            return json.dumps(serialized, separators=(",", ":")) if db_type == "sql" else serialized
        if isinstance(value, tuple):
            serialized = [cls._serialize_for_db(item, db_type) for item in value]
            return json.dumps(serialized, separators=(",", ":")) if db_type == "sql" else tuple(serialized)
        if isinstance(value, dict):
            serialized = {str(k): cls._serialize_for_db(v, db_type) for k, v in value.items()}
            return json.dumps(serialized, separators=(",", ":")) if db_type == "sql" else serialized
        return value

    @classmethod
    def _looks_like_record(cls, payload: Mapping[str, Any]) -> bool:
        fields = set(cls.get_fields().keys())
        if not fields:
            return True
        return bool(fields.intersection(payload.keys()))

    @classmethod
    def get_sensitive_fields(cls) -> set[str] | None:
        configured = getattr(cls, "__sensitive_fields__", None)
        if configured is None:
            return None
        return {str(item).strip().lower() for item in configured if str(item).strip()}

    @classmethod
    def _protect_sensitive_fields(cls, payload: Mapping[str, Any], resolved_db: Any) -> dict[str, Any]:
        settings = getattr(resolved_db, "settings", None)
        enabled = bool(getattr(settings, "hash_sensitive_fields", True))
        rounds = int(getattr(settings, "bcrypt_rounds", 12))
        return SensitiveFieldProtector.protect_mapping(
            payload,
            enabled=enabled,
            rounds=rounds,
            field_names=cls.get_sensitive_fields(),
        )

    @staticmethod
    def _is_optional_type(expected_type: Any) -> bool:
        origin = get_origin(expected_type)
        if origin in (Union, types.UnionType):
            return type(None) in get_args(expected_type)
        args = get_args(expected_type)
        if args:
            return type(None) in args
        return False

    @classmethod
    def _require_sql_backend(cls, resolved) -> None:
        if getattr(resolved, "db_type", None) != "sql":
            raise QueryError("Model schema migrations are currently supported for SQL backends only")

    @classmethod
    def _engine_for(cls, resolved):
        engine = getattr(getattr(resolved, "adapter", None), "engine", None)
        if engine is None:
            raise QueryError("Bound SQL backend does not expose a SQLAlchemy engine")
        return engine

    @classmethod
    def _declared_table(cls):
        from ...alembic_support import build_metadata_from_models

        metadata = build_metadata_from_models([cls])
        return metadata.tables[cls.get_name()]

    @classmethod
    def _schema_history_table_identifier(cls, engine) -> tuple[str, str]:
        table_name = str(cls.__schema_migrations_table__)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
            raise QueryError("Invalid schema migrations table name")
        return table_name, engine.dialect.identifier_preparer.quote(table_name)

    @classmethod
    def _ensure_schema_history_table(cls, engine) -> None:
        dialect = engine.dialect.name.lower()
        table_name, quoted_table_name = cls._schema_history_table_identifier(engine)
        create_sql_by_dialect = {
            "sqlite": f"""
                CREATE TABLE IF NOT EXISTS {quoted_table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model_name VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    operation VARCHAR(64) NOT NULL,
                    details TEXT,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """,
            "mysql": f"""
                CREATE TABLE IF NOT EXISTS {quoted_table_name} (
                    id INTEGER PRIMARY KEY AUTO_INCREMENT,
                    model_name VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    operation VARCHAR(64) NOT NULL,
                    details TEXT,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """,
            "postgresql": f"""
                CREATE TABLE IF NOT EXISTS {quoted_table_name} (
                    id SERIAL PRIMARY KEY,
                    model_name VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    operation VARCHAR(64) NOT NULL,
                    details TEXT,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            "mssql": f"""
                IF OBJECT_ID(N'{table_name}', N'U') IS NULL
                CREATE TABLE {quoted_table_name} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    model_name NVARCHAR(255) NOT NULL,
                    table_name NVARCHAR(255) NOT NULL,
                    operation NVARCHAR(64) NOT NULL,
                    details NVARCHAR(MAX) NULL,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """,
        }
        sql = create_sql_by_dialect.get(dialect, create_sql_by_dialect["sqlite"])
        with engine.begin() as conn:
            conn.execute(text(sql))

    @classmethod
    def _record_schema_migration(cls, engine, *, operation: str, details: Mapping[str, Any]) -> None:
        _, quoted_history_table = cls._schema_history_table_identifier(engine)
        sql = text(
            f"INSERT INTO {quoted_history_table} "  # nosec B608
            "(model_name, table_name, operation, details) "
            "VALUES (:model_name, :table_name, :operation, :details)"
        )
        with engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "model_name": cls.__name__,
                    "table_name": cls.get_name(),
                    "operation": operation,
                    "details": json.dumps(dict(details), default=str),
                },
            )

    @classmethod
    def _add_column(cls, engine, table_name: str, column) -> None:
        preparer = engine.dialect.identifier_preparer
        quoted_table = preparer.quote(table_name)
        compiled = str(CreateColumn(column).compile(dialect=engine.dialect))
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {quoted_table} ADD {compiled}"))

    @classmethod
    def _invalidate_sql_table_cache(cls, resolved, table_name: str) -> None:
        adapter = getattr(resolved, "adapter", None)
        table_cache = getattr(adapter, "_table_cache", None)
        if isinstance(table_cache, dict):
            table_cache.pop(table_name, None)
        column_cache = getattr(adapter, "_column_type_cache", None)
        if isinstance(column_cache, dict):
            column_cache.pop(table_name, None)
        metadata = getattr(adapter, "_metadata", None)
        if metadata is not None and hasattr(metadata, "tables"):
            table = metadata.tables.get(table_name)
            if table is not None:
                metadata.remove(table)


from typing import Generic

class ModelQueryBuilder(Generic[TModel]):
    """QueryBuilder wrapper that returns typed UModel instances.

    Wraps the standard QueryBuilder to provide model-aware results.
    All chainable methods return self, terminal methods return model instances.
    """

    def __init__(self, model_cls: type[TModel], udom) -> None:
        """Initialize with a model class and UDOM instance."""
        from ..query_builder import QueryBuilder
        self._model_cls = model_cls
        self._udom = udom
        self._builder = QueryBuilder(udom, model_cls.get_name())

    def __getattr__(self, name: str) -> Any:
        """Return model field references from a model-bound query object."""
        if name in self._model_cls.get_fields():
            return getattr(self._model_cls, name)
        raise AttributeError(name)

    def where(self, conditions: Mapping[str, Any] | str | None = None, **kwargs: Any) -> "ModelQueryBuilder[TModel]":
        """Add WHERE conditions."""
        self._builder.where(conditions, **kwargs)
        return self

    def where_or(self, *condition_groups: Mapping[str, Any]) -> "ModelQueryBuilder[TModel]":
        """Add OR conditions."""
        self._builder.where_or(*condition_groups)
        return self

    def where_in(self, field: str, values: list[Any]) -> "ModelQueryBuilder[TModel]":
        """Add IN condition."""
        self._builder.where_in(field, values)
        return self

    def where_not(self, field: Any = None, value: Any = None, **kwargs: Any) -> "ModelQueryBuilder[TModel]":
        """Add NOT conditions."""
        self._builder.where_not(field, value, **kwargs)
        return self

    def where_gt(self, field: Any = None, value: Any = None, **kwargs: Any) -> "ModelQueryBuilder[TModel]":
        """Add greater-than conditions."""
        self._builder.where_gt(field, value, **kwargs)
        return self

    def where_gte(self, field: Any = None, value: Any = None, **kwargs: Any) -> "ModelQueryBuilder[TModel]":
        """Add greater-than-or-equal conditions."""
        self._builder.where_gte(field, value, **kwargs)
        return self

    def where_lt(self, field: Any = None, value: Any = None, **kwargs: Any) -> "ModelQueryBuilder[TModel]":
        """Add less-than conditions."""
        self._builder.where_lt(field, value, **kwargs)
        return self

    def where_lte(self, field: Any = None, value: Any = None, **kwargs: Any) -> "ModelQueryBuilder[TModel]":
        """Add less-than-or-equal conditions."""
        self._builder.where_lte(field, value, **kwargs)
        return self

    def where_like(self, field: Any = None, value: Any = None, **kwargs: Any) -> "ModelQueryBuilder[TModel]":
        """Add LIKE conditions."""
        self._builder.where_like(field, value, **kwargs)
        return self

    def where_null(self, *fields: str) -> "ModelQueryBuilder[TModel]":
        """Add IS NULL conditions."""
        self._builder.where_null(*fields)
        return self

    def where_not_null(self, *fields: str) -> "ModelQueryBuilder[TModel]":
        """Add IS NOT NULL conditions."""
        self._builder.where_not_null(*fields)
        return self

    def select(self, *fields: str) -> "ModelQueryBuilder[TModel]":
        """Specify fields to return."""
        self._builder.select(*fields)
        return self

    def order(self, field: str, direction: str = "ASC") -> "ModelQueryBuilder[TModel]":
        """Set ORDER BY."""
        self._builder.order(field, direction)
        return self

    def order_by(self, order_expr: str) -> "ModelQueryBuilder[TModel]":
        """Set ORDER BY with raw expression."""
        self._builder.order_by(order_expr)
        return self

    def limit(self, count: int) -> "ModelQueryBuilder[TModel]":
        """Set result limit."""
        self._builder.limit(count)
        return self

    def offset(self, count: int) -> "ModelQueryBuilder[TModel]":
        """Set result offset."""
        self._builder.offset(count)
        return self

    def page(self, page_num: int, page_size: int = 20) -> "ModelQueryBuilder[TModel]":
        """Set pagination."""
        self._builder.page(page_num, page_size)
        return self

    def group_by(self, *fields: str) -> "ModelQueryBuilder[TModel]":
        """Set GROUP BY fields."""
        self._builder.group_by(*fields)
        return self

    def join(
        self,
        entity: str,
        *,
        on: Mapping[str, str] | tuple[str, str] | list[str],
        join_type: str = "inner",
    ) -> "ModelQueryBuilder[TModel]":
        """Add a SQL join clause."""
        self._builder.join(entity, on=on, join_type=join_type)
        return self

    def left_join(
        self,
        entity: str,
        *,
        on: Mapping[str, str] | tuple[str, str] | list[str],
    ) -> "ModelQueryBuilder[TModel]":
        """Add a SQL LEFT JOIN clause."""
        self._builder.left_join(entity, on=on)
        return self

    def having(self, conditions: Mapping[str, Any] | str) -> "ModelQueryBuilder[TModel]":
        """Set HAVING conditions."""
        self._builder.having(conditions)
        return self

    def metrics(self, **kwargs: Any) -> "ModelQueryBuilder[TModel]":
        """Set aggregation metrics."""
        self._builder.metrics(**kwargs)
        return self

    # ─────────────────────────────────────────────────────────────────────────
    # Terminal Methods - return model instances
    # ─────────────────────────────────────────────────────────────────────────

    def _hydrate(self, row: Mapping[str, Any]) -> TModel:
        """Hydrate a full or projected row using the appropriate validation."""
        return self._model_cls.from_dict(row, partial=bool(self._builder._select_fields))

    def find(self) -> list[TModel]:
        """Execute query and return list of model instances."""
        rows = self._builder.find()
        return [self._hydrate(row) for row in rows if isinstance(row, Mapping)]

    def first(self) -> TModel | None:
        """Execute query and return first model instance or None."""
        row = self._builder.first()
        if row is None:
            return None
        return self._hydrate(row) if isinstance(row, Mapping) else None

    def count(self) -> int:
        """Return count of matching records."""
        return self._builder.count()

    def exists(self) -> bool:
        """Check if any records match."""
        return self._builder.exists()

    def update(self, data: Mapping[str, Any]) -> Any:
        """Update matching records."""
        return self._builder.update(data)

    def delete(self) -> Any:
        """Delete matching records."""
        return self._builder.delete()

    def aggregate(
        self,
        *,
        metrics: Mapping[str, Any] | None = None,
        pipeline: list[Mapping[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute aggregation query."""
        return self._builder.aggregate(metrics=metrics, pipeline=pipeline)

    def find_page(self, page: int = 1, page_size: int = 20) -> dict[str, Any]:
        """Execute paginated query with model instances."""
        result = self._builder.find_page(page=page, page_size=page_size)
        result["items"] = [
            self._hydrate(item) for item in result.get("items", []) if isinstance(item, Mapping)
        ]
        return result

    def clone(self) -> "ModelQueryBuilder[TModel]":
        """Create a copy of this builder."""
        new_builder = ModelQueryBuilder(self._model_cls, self._udom)
        new_builder._builder = self._builder.clone()
        return new_builder

    def to_dict(self) -> dict[str, Any]:
        """Return query state as dictionary."""
        return self._builder.to_dict()

    def __repr__(self) -> str:
        """Return string representation."""
        return f"ModelQueryBuilder({self._model_cls.__name__}, {repr(self._builder)})"

