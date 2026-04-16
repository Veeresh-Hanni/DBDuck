"""Alembic support helpers for DBDuck model metadata."""

from __future__ import annotations

from importlib import import_module
from datetime import date, datetime, time
from typing import Any, Iterable, get_args

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

from DBDuck.models import Column as DBDuckColumn
from DBDuck.models import ForeignKey as DBDuckForeignKey
from DBDuck.udom.models.umodel import UModel


def _is_model_class(candidate: Any) -> bool:
    return isinstance(candidate, type) and issubclass(candidate, UModel) and candidate is not UModel


def load_model_classes(module_name: str, model_names: Iterable[str] | None = None) -> list[type[UModel]]:
    module = import_module(module_name)
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


def _sa_type_for(annotation: Any) -> Any:
    resolved = _optional_inner_type(annotation)
    python_type = getattr(resolved, "python_type", None)
    if python_type is None and isinstance(resolved, type):
        python_type = resolved
    if python_type is str:
        return SAString()
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
    if resolved in {SAString, SAInteger, SAFloat, SABoolean, SAJSON}:
        return resolved()
    return SAString()


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


def _column_from_descriptor(name: str, descriptor: DBDuckColumn) -> SAColumn:
    kwargs: dict[str, Any] = {
        "nullable": bool(descriptor.nullable),
        "primary_key": bool(descriptor.primary_key),
        "unique": bool(descriptor.unique),
    }
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

        Table(table_name, metadata, *sa_columns)
    return metadata
