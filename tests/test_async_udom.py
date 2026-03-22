"""AsyncUDOM regression tests."""

from __future__ import annotations

import pytest
from uuid import uuid4

from DBDuck.udom.async_udom import AsyncUDOM

pytest.importorskip("pytest_asyncio")


def _async_sqlite_url() -> str:
    memory_name = f"dbduck_async_{uuid4().hex}"
    return f"sqlite:///file:{memory_name}?mode=memory&cache=shared&uri=true"


@pytest.mark.asyncio
async def test_async_create_find_update_delete_roundtrip() -> None:
    db = AsyncUDOM(db_type="sql", db_instance="sqlite", url=_async_sqlite_url())
    try:
        created = await db.create("users", {"id": 1, "name": "alice", "active": True})
        assert created["rows_affected"] == 1

        found = await db.find("users", where={"id": 1})
        assert len(found) == 1
        assert found[0]["name"] == "alice"

        updated = await db.update("users", {"name": "bob"}, where={"id": 1})
        assert updated["rows_affected"] == 1
        assert (await db.find("users", where={"id": 1}))[0]["name"] == "bob"

        deleted = await db.delete("users", where={"id": 1})
        assert deleted["rows_affected"] == 1
        assert await db.count("users") == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_async_transaction_commit() -> None:
    db = AsyncUDOM(db_type="sql", db_instance="sqlite", url=_async_sqlite_url())
    try:
        async with db.transaction():
            await db.create("orders", {"id": 1, "paid": True})
        assert await db.count("orders") == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_async_transaction_rollback() -> None:
    db = AsyncUDOM(db_type="sql", db_instance="sqlite", url=_async_sqlite_url())
    try:
        with pytest.raises(RuntimeError):
            async with db.transaction():
                await db.create("orders", {"id": 1, "paid": True})
                raise RuntimeError("boom")
        assert await db.count("orders") == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_async_find_page() -> None:
    db = AsyncUDOM(db_type="sql", db_instance="sqlite", url=_async_sqlite_url())
    try:
        await db.create_many("users", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}])
        page = await db.find_page("users", page=1, page_size=2, order_by="id ASC")
        assert page["total"] == 3
        assert len(page["items"]) == 2
    finally:
        await db.close()
