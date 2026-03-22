"""Async UDOM facade that preserves the sync UDOM API shape with awaitable methods."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Mapping

from ..core.exceptions import QueryError
from ..utils.logger import get_logger
from .udom import UDOM


class _AsyncTransactionContext:
    """Async context manager that delegates transaction control to AsyncUDOM."""

    def __init__(self, db: "AsyncUDOM") -> None:
        self._db = db

    async def __aenter__(self) -> "AsyncUDOM":
        await self._db.begin()
        return self._db

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            await self._db.commit()
        else:
            await self._db.rollback()


class AsyncUDOM:
    """Async wrapper around the hardened UDOM core.

    The public API mirrors :class:`DBDuck.udom.udom.UDOM`, but every method is awaitable.
    A dedicated single-thread executor preserves SQLite in-memory consistency while reusing the
    same security and validation controls as the synchronous implementation.
    """

    def __init__(
        self,
        db_type: str = "sql",
        db_instance: str | None = None,
        server: str | None = None,
        url: str | None = None,
        **options: Any,
    ) -> None:
        self._logger = get_logger(options.get("log_level"))
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbduck-async")
        self._sync = UDOM(db_type=db_type, db_instance=db_instance, server=server, url=url, **options)

    async def _call(self, func, /, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        callback = partial(func, *args, **kwargs)
        return await loop.run_in_executor(self._executor, callback)

    @property
    def db_type(self) -> str:
        return self._sync.db_type

    @property
    def db_instance(self) -> str:
        return self._sync.db_instance

    @property
    def url(self) -> str | None:
        return self._sync.url

    @property
    def settings(self):
        return self._sync.settings

    @property
    def adapter(self):
        return self._sync.adapter

    async def query(self, query: str) -> Any:
        return await self._call(self._sync.query, query)

    async def execute(self, query: str) -> Any:
        return await self._call(self._sync.execute, query)

    async def uquery(self, uql: str) -> Any:
        return await self._call(self._sync.uquery, uql)

    async def uexecute(self, uql: str) -> Any:
        return await self._call(self._sync.uexecute, uql)

    async def create(self, entity: str, data: Mapping[str, Any]) -> Any:
        return await self._call(self._sync.create, entity, data)

    async def create_many(self, entity: str, rows: list[Mapping[str, Any]]) -> Any:
        return await self._call(self._sync.create_many, entity, rows)

    async def find(
        self,
        entity: str,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> Any:
        return await self._call(self._sync.find, entity, where=where, order_by=order_by, limit=limit)

    async def find_page(
        self,
        entity: str,
        page: int = 1,
        page_size: int = 20,
        where: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        return await self._call(
            self._sync.find_page,
            entity,
            page=page,
            page_size=page_size,
            where=where,
            order_by=order_by,
        )

    async def update(self, entity: str, data: Mapping[str, Any], where: Mapping[str, Any] | str) -> Any:
        return await self._call(self._sync.update, entity, data, where)

    async def delete(self, entity: str, where: Mapping[str, Any] | str) -> Any:
        return await self._call(self._sync.delete, entity, where)

    async def count(self, entity: str, where: Mapping[str, Any] | str | None = None) -> int:
        return int(await self._call(self._sync.count, entity, where))

    async def aggregate(
        self,
        entity: str,
        *,
        group_by: str | list[str] | tuple[str, ...] | None = None,
        metrics: Mapping[str, Any] | None = None,
        where: Mapping[str, Any] | str | None = None,
        having: Mapping[str, Any] | str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        pipeline: list[Mapping[str, Any]] | None = None,
    ) -> Any:
        return await self._call(
            self._sync.aggregate,
            entity,
            group_by=group_by,
            metrics=metrics,
            where=where,
            having=having,
            order_by=order_by,
            limit=limit,
            pipeline=pipeline,
        )

    async def begin(self) -> Any:
        return await self._call(self._sync.begin)

    async def commit(self) -> None:
        await self._call(self._sync.commit)

    async def rollback(self) -> None:
        await self._call(self._sync.rollback)

    def transaction(self) -> _AsyncTransactionContext:
        return _AsyncTransactionContext(self)

    async def ping(self) -> Any:
        return await self._call(self._sync.ping)

    async def close(self) -> None:
        try:
            await self._call(self._sync.close)
        finally:
            self._executor.shutdown(wait=True, cancel_futures=False)

    async def ensure_indexes(self, entity: str, indexes: list[Mapping[str, Any]]) -> Any:
        return await self._call(self._sync.ensure_indexes, entity, indexes)

    async def create_view(self, name: str, select_query: str, *, replace: bool = False) -> Any:
        return await self._call(self._sync.create_view, name, select_query, replace=replace)

    async def drop_view(self, name: str, *, if_exists: bool = True) -> Any:
        return await self._call(self._sync.drop_view, name, if_exists=if_exists)

    async def create_procedure(self, name: str, definition: str, *, replace: bool = False) -> Any:
        return await self._call(self._sync.create_procedure, name, definition, replace=replace)

    async def drop_procedure(self, name: str, *, if_exists: bool = True) -> Any:
        return await self._call(self._sync.drop_procedure, name, if_exists=if_exists)

    async def call_procedure(self, name: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        return await self._call(self._sync.call_procedure, name, params=params)

    async def create_function(self, name: str, definition: str, *, replace: bool = False) -> Any:
        return await self._call(self._sync.create_function, name, definition, replace=replace)

    async def drop_function(self, name: str, *, if_exists: bool = True) -> Any:
        return await self._call(self._sync.drop_function, name, if_exists=if_exists)

    async def call_function(self, name: str, params: list[Any] | tuple[Any, ...] | None = None) -> Any:
        return await self._call(self._sync.call_function, name, params=params)

    async def create_event(
        self,
        name: str,
        schedule: str,
        body: str,
        *,
        replace: bool = False,
        preserve: bool = True,
        enable: bool = True,
    ) -> Any:
        return await self._call(
            self._sync.create_event,
            name,
            schedule,
            body,
            replace=replace,
            preserve=preserve,
            enable=enable,
        )

    async def drop_event(self, name: str, *, if_exists: bool = True) -> Any:
        return await self._call(self._sync.drop_event, name, if_exists=if_exists)

    async def verify_secret(self, plain_value: Any, stored_hash: Any) -> bool:
        return bool(await self._call(self._sync.verify_secret, plain_value, stored_hash))

    async def create_relationship(
        self,
        from_label: str,
        from_id: Any,
        rel_type: str,
        to_label: str,
        to_id: Any,
        props: Mapping[str, Any] | None = None,
    ) -> Any:
        adapter_method = getattr(self._sync.adapter, "create_relationship", None)
        if adapter_method is None:
            raise QueryError("Current adapter does not support create_relationship")
        return await self._call(adapter_method, from_label, from_id, rel_type, to_label, to_id, props)

    async def find_related(
        self,
        entity: str,
        id: Any,
        rel_type: str,
        direction: str = "out",
        target_label: str | None = None,
    ) -> Any:
        adapter_method = getattr(self._sync.adapter, "find_related", None)
        if adapter_method is None:
            raise QueryError("Current adapter does not support find_related")
        return await self._call(adapter_method, entity, id, rel_type, direction, target_label)

    async def shortest_path(self, from_label: str, from_id: Any, to_label: str, to_id: Any) -> Any:
        adapter_method = getattr(self._sync.adapter, "shortest_path", None)
        if adapter_method is None:
            raise QueryError("Current adapter does not support shortest_path")
        return await self._call(adapter_method, from_label, from_id, to_label, to_id)

    async def create_collection(self, entity: str, vector_size: int, distance: str = "cosine") -> Any:
        adapter_method = getattr(self._sync.adapter, "create_collection", None)
        if adapter_method is None:
            raise QueryError("Current adapter does not support create_collection")
        return await self._call(adapter_method, entity, vector_size, distance)

    async def collection_info(self, entity: str) -> Any:
        adapter_method = getattr(self._sync.adapter, "collection_info", None)
        if adapter_method is None:
            raise QueryError("Current adapter does not support collection_info")
        return await self._call(adapter_method, entity)

    async def upsert_vector(self, entity: str, id: Any, vector: Any, metadata: Mapping[str, Any] | None = None) -> Any:
        adapter_method = getattr(self._sync.adapter, "upsert_vector", None)
        if adapter_method is None:
            raise QueryError("Current adapter does not support upsert_vector")
        return await self._call(adapter_method, entity, id, vector, metadata)

    async def search_similar(
        self,
        entity: str,
        vector: Any,
        top_k: int = 10,
        filter: Mapping[str, Any] | None = None,
    ) -> Any:
        adapter_method = getattr(self._sync.adapter, "search_similar", None)
        if adapter_method is None:
            raise QueryError("Current adapter does not support search_similar")
        return await self._call(adapter_method, entity, vector, top_k, filter)

    async def delete_vector(self, entity: str, id: Any) -> Any:
        adapter_method = getattr(self._sync.adapter, "delete_vector", None)
        if adapter_method is None:
            raise QueryError("Current adapter does not support delete_vector")
        return await self._call(adapter_method, entity, id)

    async def __aenter__(self) -> "AsyncUDOM":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

