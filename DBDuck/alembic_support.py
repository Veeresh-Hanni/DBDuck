"""Alembic support helpers for DBDuck model metadata."""

from __future__ import annotations

from datetime import date, datetime, time
from importlib import import_module
from typing import Any, Iterable, get_args

import sqlalchemy as sa
from sqlalchemy import Boolean as SABoolean
from sqlalchemy import Column as SAColumn
from sqlalchemy import DateTime as SADateTime
from sqlalchemy import Float as SAFloat
from sqlalchemy import ForeignKey as SAForeignKey
from sqlalchemy import Integer as SAInteger
from sqlalchemy import JSON as SAJSON
from sqlalchemy import MetaData
from sqlalchemy import String as SAString
from sqlalchemy import Table
from sqlalchemy import Text as SAText
from sqlalchemy import text as sa_text

from DBDuck.models import Column as DBDuckColumn
from DBDuck.models import ForeignKey as DBDuckForeignKey
from DBDuck.models import _UNSET as DBDuckUnset
from DBDuck.udom.models.umodel import UModel


def _is_model_class(candidate: Any) -> bool:
    return isinstance(candidate, type) and issubclass(candidate, UModel) and candidate is not UModel


def apply_sqlalchemy_migration_compat(dialect_name: str) -> None:
    if (dialect_name or "").lower() != "mysql":
        return
    if getattr(sa.String, "__name__", "") == "DBDuckMySQLString":
        return

    original_string = sa.String

    class DBDuckMySQLString(original_string):
        def __init__(self, length: int | None = None, *args: Any, **kwargs: Any) -> None:
            super().__init__(length=255 if length is None else length, *args, **kwargs)

    sa.String = DBDuckMySQLString


def migration_context_options(dialect_name: str) -> dict[str, bool]:
    """Return Alembic options required by the selected SQL dialect."""
    normalized = (dialect_name or "").lower()
    return {
        "compare_type": True,
        # SQLite cannot perform many ALTER TABLE operations directly.
        # Batch mode recreates the table and copies its existing rows.
        "render_as_batch": normalized == "sqlite",
    }


def load_model_classes(module_name: str, model_names: Iterable[str] | None = None) -> list[type[UModel]]:
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            f"Failed to import module '{module_name}'. "
            "Run the command from your project root or pass --project-dir explicitly."
        ) from exc
    if model_names:
        resolved: list[type[UModel]] = []
        for name in model_names:
            candidate = getattr(module, name, None)
            if not _is_model_class(candidate):
                raise ImportError(f"Model '{name}' was not found in module '{module_name}'")
            resolved.append(candidate)
        return resolved

    models = [
        value
        for value in module.__dict__.values()
        if _is_model_class(value) and getattr(value, "__module__", None) == module.__name__
    ]
    models.sort(key=lambda item: item.__name__)
    return models


def _optional_inner_type(annotation: Any) -> Any:
    args = get_args(annotation)
    if args and type(None) in args:
        return next(arg for arg in args if arg is not type(None))
    return annotation


def _string_length_for(resolved: Any) -> int | None:
    length = getattr(resolved, "length", None)
    if length is not None:
        return int(length)
    if isinstance(resolved, type):
        return int(getattr(resolved, "length", 255))
    return 255


def _sa_type_for(annotation: Any) -> Any:
    resolved = _optional_inner_type(annotation)
    resolved_name = getattr(resolved, "__name__", resolved.__class__.__name__)
    if resolved_name in {"DateTime", "DateTimeField"}:
        return SADateTime()
    if resolved_name in {"Text", "TextField"}:
        return SAText()
    python_type = getattr(resolved, "python_type", None)
    if python_type is None and isinstance(resolved, type):
        python_type = resolved
    if python_type is str:
        return SAString(length=_string_length_for(resolved))
    if python_type is int:
        return SAInteger()
    if python_type is float:
        return SAFloat()
    if python_type is bool:
        return SABoolean()
    if python_type in {date, datetime, time}:
        return SADateTime()
    if python_type in {dict, list, tuple, set}:
        return SAJSON()
    if resolved is SAString:
        return SAString(length=255)
    if resolved in {SAInteger, SAFloat, SABoolean, SAJSON}:
        return resolved()
    return SAString(length=255)


def _annotation_is_optional(annotation: Any) -> bool:
    args = getattr(annotation, "__args__", ())
    return bool(args) and type(None) in args


def _column_from_annotation(name: str, annotation: Any) -> SAColumn:
    resolved = _optional_inner_type(annotation)
    kwargs: dict[str, Any] = {"nullable": _annotation_is_optional(annotation)}
    if name in {"id", "_id"}:
        kwargs["primary_key"] = True
        kwargs["nullable"] = False
    return SAColumn(name, _sa_type_for(resolved), **kwargs)


def _server_default_for(value: Any, type_spec: Any = None):
    if value is DBDuckUnset or callable(value):
        return None
    if value is None:
        return None
    type_name = getattr(type_spec, "__name__", type_spec.__class__.__name__) if type_spec is not None else ""
    if type_name in {"DateTime", "DateTimeField"}:
        if isinstance(value, str) and not value.strip():
            return None
        if isinstance(value, str) and value.strip().upper() == "CURRENT_TIMESTAMP":
            return sa_text("CURRENT_TIMESTAMP")
        return None
    if isinstance(value, bool):
        return sa.true() if value else sa.false()
    if isinstance(value, (int, float)):
        return sa_text(str(value))
    text_value = str(value).replace("'", "''")
    return sa_text(f"'{text_value}'")


def _column_from_descriptor(name: str, descriptor: DBDuckColumn) -> SAColumn:
    kwargs: dict[str, Any] = {
        "nullable": bool(descriptor.nullable),
        "primary_key": bool(descriptor.primary_key),
        "unique": bool(descriptor.unique),
    }
    server_default = _server_default_for(getattr(descriptor, "default", DBDuckUnset), descriptor.type_)
    if server_default is not None:
        kwargs["server_default"] = server_default
    if isinstance(descriptor, DBDuckForeignKey):
        target = descriptor.to.get_name()
        kwargs["nullable"] = bool(descriptor.nullable)
        return SAColumn(
            name,
            _sa_type_for(descriptor.type_),
            SAForeignKey(f"{target}.{descriptor.to_field}"),
            **kwargs,
        )
    return SAColumn(name, _sa_type_for(descriptor.type_), **kwargs)


def build_metadata_from_models(model_classes: Iterable[type[UModel]]) -> MetaData:
    metadata = MetaData()
    for model_cls in model_classes:
        column_descriptors = getattr(model_cls, "__columns__", {}) or {}
        annotations = dict(getattr(model_cls, "get_fields", lambda: {})())
        table_name = model_cls.get_name()
        sa_columns: list[SAColumn] = []

        if column_descriptors:
            for field_name, descriptor in column_descriptors.items():
                sa_columns.append(_column_from_descriptor(field_name, descriptor))
        else:
            for field_name, annotation in annotations.items():
                if field_name.startswith("_"):
                    continue
                sa_columns.append(_column_from_annotation(field_name, annotation))

        table = Table(table_name, metadata, *sa_columns)

        # NEW: build indexes from __indexes__
        index_specs = getattr(model_cls, "__indexes__", []) or []
        for idx, spec in enumerate(index_specs):
            columns = spec if isinstance(spec, (tuple, list)) else (spec,)
            index_name = f"ix_{table_name}_" + "_".join(columns)
            sa.Index(index_name, *[table.c[col] for col in columns])

    return metadata
