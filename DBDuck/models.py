"""Django-style declarative model compatibility on top of DBDuck UModel."""

from __future__ import annotations

from typing import Any, Callable

from sqlalchemy import inspect as sa_inspect

from DBDuck.core.exceptions import QueryError
from DBDuck.udom.models.umodel import UModel as _CoreUModel


_UNSET = object()
CASCADE = "CASCADE"
SET_NULL = "SET_NULL"
RESTRICT = "RESTRICT"
DO_NOTHING = "DO_NOTHING"


class _TypeSpec:
    python_type: Any = Any

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass


class String(_TypeSpec):
    python_type = str

    def __init__(self, length: int | None = 255, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.length = length


class Integer(_TypeSpec):
    python_type = int


class Float(_TypeSpec):
    python_type = float


class Boolean(_TypeSpec):
    python_type = bool


class JSON(_TypeSpec):
    python_type = dict


class DateTime(_TypeSpec):
    # Stored as text by default SQL adapters; keep validation compatible.
    python_type = str


class AutoField(_TypeSpec):
    python_type = int


class Column:
    """Declarative field descriptor for Django-style model definitions."""

    def __init__(
        self,
        type_: Any,
        *,
        primary_key: bool = False,
        nullable: bool = False,
        default: Any = _UNSET,
        unique: bool = False,
    ) -> None:
        self.type_ = type_
        self.primary_key = bool(primary_key)
        self.nullable = bool(nullable)
        self.default = default
        self.unique = bool(unique)
        self.name: str | None = None

    def __set_name__(self, owner, name: str) -> None:
        self.name = name

    def _default_value(self) -> Any:
        if self.default is _UNSET:
            return _UNSET
        if callable(self.default):
            return self.default()
        return self.default

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if self.name in instance.__dict__:
            return instance.__dict__[self.name]
        value = self._default_value()
        if value is _UNSET:
            raise AttributeError(self.name or "field")
        instance.__dict__[self.name] = value
        return value

    def __set__(self, instance, value: Any) -> None:
        instance.__dict__[self.name] = value


def _resolve_python_type(type_spec: Any) -> Any:
    if isinstance(type_spec, _TypeSpec):
        return type_spec.python_type
    if isinstance(type_spec, type) and issubclass(type_spec, _TypeSpec):
        return type_spec.python_type
    if isinstance(type_spec, type):
        return type_spec
    if hasattr(type_spec, "python_type"):
        return getattr(type_spec, "python_type")
    return Any


def _annotation_for_column(column: Column) -> Any:
    base = _resolve_python_type(column.type_)
    if column.nullable:
        return base | None
    return base


class _ModelMeta(type):
    def __new__(mcls, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
        annotations = dict(namespace.get("__annotations__", {}))
        inherited: dict[str, Column] = {}
        for base in bases:
            inherited.update(getattr(base, "__columns__", {}))

        current: dict[str, Column] = {}
        for field_name, value in namespace.items():
            if isinstance(value, Column):
                current[field_name] = value
                if field_name not in annotations:
                    annotations[field_name] = _annotation_for_column(value)

        all_columns = {**inherited, **current}
        namespace["__annotations__"] = annotations
        namespace["__columns__"] = all_columns
        namespace["__pk_field__"] = next((k for k, v in all_columns.items() if getattr(v, "primary_key", False)), None)

        meta = namespace.get("Meta")
        if meta is not None and hasattr(meta, "db_table") and "__table__" not in namespace and "__entity__" not in namespace:
            namespace["__table__"] = str(getattr(meta, "db_table"))

        cls = super().__new__(mcls, name, bases, namespace)
        cls.objects = Manager(cls)
        return cls


class Manager:
    """Small Django-style manager facade over the bound UModel class."""

    def __init__(self, model_cls: type["UModel"]) -> None:
        self.model_cls = model_cls

    def all(self):
        return self.model_cls.find()

    def filter(self, **kwargs: Any):
        return self.model_cls.find(where=kwargs or None)

    def get(self, **kwargs: Any):
        obj = self.model_cls.find_one(where=kwargs or None)
        if obj is None:
            raise QueryError(f"{self.model_cls.__name__} matching query does not exist")
        return obj

    def create(self, **kwargs: Any):
        instance = self.model_cls(**kwargs)
        instance.save()
        return instance

    def count(self, **kwargs: Any) -> int:
        return self.model_cls.count(where=kwargs or None)

    def bulk_create(self, objs: list["UModel | dict[str, Any]"]):
        return self.model_cls.bulk_create(objs)

    def get_or_create(self, defaults: dict[str, Any] | None = None, **lookup: Any):
        obj = self.model_cls.find_one(where=lookup or None)
        if obj is not None:
            return obj, False
        payload = {**lookup, **(defaults or {})}
        created = self.model_cls(**payload)
        created.save()
        return created, True

    def update_or_create(self, defaults: dict[str, Any] | None = None, **lookup: Any):
        obj = self.model_cls.find_one(where=lookup or None)
        if obj is not None:
            update_payload = dict(defaults or {})
            if update_payload:
                obj.update(data=update_payload, where=lookup)
                for key, value in update_payload.items():
                    setattr(obj, key, value)
            return obj, False
        payload = {**lookup, **(defaults or {})}
        created = self.model_cls(**payload)
        created.save()
        return created, True


class UModel(_CoreUModel, metaclass=_ModelMeta):
    """UModel compatibility class with declarative Column fields."""

    __columns__: dict[str, Column] = {}

    def __init__(self, **kwargs: Any) -> None:
        for name, column in self.__columns__.items():
            if name in kwargs:
                setattr(self, name, kwargs.pop(name))
                continue
            default = column._default_value()
            if default is not _UNSET:
                setattr(self, name, default)
        super().__init__(**kwargs)

    @classmethod
    def pk_field(cls) -> str | None:
        return getattr(cls, "__pk_field__", None)

    @property
    def pk(self) -> Any:
        field = self.__class__.pk_field()
        return getattr(self, field) if field and hasattr(self, field) else None

    def save(self, db=None):
        pk_field = self.__class__.pk_field()
        resolved = self._resolve_instance_db(db)
        if pk_field and hasattr(self, pk_field):
            pk_value = getattr(self, pk_field)
            if pk_value is not None:
                engine = getattr(getattr(resolved, "adapter", None), "engine", None)
                table_exists = False
                if engine is not None:
                    try:
                        table_exists = self.get_name() in set(sa_inspect(engine).get_table_names())
                    except Exception:
                        table_exists = False
                if table_exists:
                    existing = self.__class__.find_one(where={pk_field: pk_value}, db=resolved)
                    if existing is not None:
                        return self.update(data=self.to_dict(), where={pk_field: pk_value}, db=resolved)
        return super().save(db=resolved)

    def refresh_from_db(self, db=None) -> "UModel":
        pk_field = self.__class__.pk_field()
        if not pk_field or not hasattr(self, pk_field):
            raise QueryError("refresh_from_db requires a primary key field")
        pk_value = getattr(self, pk_field)
        fresh = self.__class__.find_one(where={pk_field: pk_value}, db=db)
        if fresh is None:
            raise QueryError(f"{self.__class__.__name__} matching query does not exist")
        for key, value in fresh.to_dict(include_none=True).items():
            setattr(self, key, value)
        return self


class ForeignKey(Column):
    """Foreign key field storing referenced object key value."""

    def __init__(
        self,
        to: type[UModel],
        *,
        on_delete: str = RESTRICT,
        to_field: str = "id",
        nullable: bool = False,
        default: Any = _UNSET,
    ) -> None:
        super().__init__(Integer, nullable=nullable, default=default)
        self.to = to
        self.to_field = to_field
        self.on_delete = on_delete

    def __set__(self, instance, value: Any) -> None:
        if value is None:
            if not self.nullable:
                raise ValueError(f"ForeignKey '{self.name}' cannot be None")
            instance.__dict__[self.name] = None
            return
        if isinstance(value, self.to):
            if not hasattr(value, self.to_field):
                raise ValueError(f"Related model has no field '{self.to_field}'")
            instance.__dict__[self.name] = getattr(value, self.to_field)
            return
        instance.__dict__[self.name] = value


# Django-style aliases
class CharField(String):
    pass


class TextField(String):
    pass


class IntegerField(Integer):
    pass


class FloatField(Float):
    pass


class BooleanField(Boolean):
    pass


class JSONField(JSON):
    pass


class DateTimeField(DateTime):
    pass


def _resolve_model_ref(model_ref: Any) -> type[UModel]:
    if isinstance(model_ref, type) and issubclass(model_ref, UModel):
        return model_ref
    if callable(model_ref):
        resolved = model_ref()
        if isinstance(resolved, type) and issubclass(resolved, UModel):
            return resolved
    raise TypeError("Relation target must be a UModel class or callable returning one")


class _Relation:
    def __init__(self) -> None:
        self.name: str | None = None

    def __set_name__(self, owner, name: str) -> None:
        self.name = name


class ManyToOne(_Relation):
    """Resolve a single parent record via local foreign key field."""

    def __init__(self, to: type[UModel] | Callable[[], type[UModel]], *, fk_field: str, to_field: str = "id") -> None:
        super().__init__()
        self.to_ref = to
        self.fk_field = fk_field
        self.to_field = to_field

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if not hasattr(instance, self.fk_field):
            return None
        fk_value = getattr(instance, self.fk_field)
        if fk_value is None:
            return None
        model_cls = _resolve_model_ref(self.to_ref)
        return model_cls.find_one(where={self.to_field: fk_value})


class OneToOne(_Relation):
    """Resolve one related record by matching foreign key on target model."""

    def __init__(
        self,
        to: type[UModel] | Callable[[], type[UModel]],
        *,
        foreign_key: str,
        local_key: str = "id",
    ) -> None:
        super().__init__()
        self.to_ref = to
        self.foreign_key = foreign_key
        self.local_key = local_key

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if not hasattr(instance, self.local_key):
            return None
        local_value = getattr(instance, self.local_key)
        if local_value is None:
            return None
        model_cls = _resolve_model_ref(self.to_ref)
        return model_cls.find_one(where={self.foreign_key: local_value})


class OneToMany(_Relation):
    """Resolve many child records by matching foreign key on target model."""

    def __init__(
        self,
        to: type[UModel] | Callable[[], type[UModel]],
        *,
        foreign_key: str,
        local_key: str = "id",
        order_by: str | None = None,
    ) -> None:
        super().__init__()
        self.to_ref = to
        self.foreign_key = foreign_key
        self.local_key = local_key
        self.order_by = order_by

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if not hasattr(instance, self.local_key):
            return []
        local_value = getattr(instance, self.local_key)
        if local_value is None:
            return []
        model_cls = _resolve_model_ref(self.to_ref)
        return model_cls.find(where={self.foreign_key: local_value}, order_by=self.order_by)


class ManyToMany(_Relation):
    """Resolve many records through a join/through model."""

    def __init__(
        self,
        to: type[UModel] | Callable[[], type[UModel]],
        *,
        through: type[UModel] | Callable[[], type[UModel]],
        from_key: str,
        to_key: str,
        local_key: str = "id",
        to_field: str = "id",
    ) -> None:
        super().__init__()
        self.to_ref = to
        self.through_ref = through
        self.from_key = from_key
        self.to_key = to_key
        self.local_key = local_key
        self.to_field = to_field

    def __get__(self, instance, owner):
        if instance is None:
            return self
        if not hasattr(instance, self.local_key):
            return []
        local_value = getattr(instance, self.local_key)
        if local_value is None:
            return []

        through_cls = _resolve_model_ref(self.through_ref)
        links = through_cls.find(where={self.from_key: local_value})
        if not links:
            return []

        target_ids: list[Any] = []
        for link in links:
            if hasattr(link, self.to_key):
                target_ids.append(getattr(link, self.to_key))
        if not target_ids:
            return []

        to_cls = _resolve_model_ref(self.to_ref)
        results = []
        seen = set()
        for target_id in target_ids:
            if target_id in seen:
                continue
            seen.add(target_id)
            obj = to_cls.find_one(where={self.to_field: target_id})
            if obj is not None:
                results.append(obj)
        return results


__all__ = [
    "UModel",
    "Column",
    "ForeignKey",
    "ManyToOne",
    "OneToOne",
    "OneToMany",
    "ManyToMany",
    "String",
    "CharField",
    "TextField",
    "Integer",
    "IntegerField",
    "Float",
    "FloatField",
    "Boolean",
    "BooleanField",
    "JSON",
    "JSONField",
    "DateTime",
    "DateTimeField",
    "AutoField",
    "CASCADE",
    "SET_NULL",
    "RESTRICT",
    "DO_NOTHING",
]
