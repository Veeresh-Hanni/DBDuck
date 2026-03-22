"""Vector adapter tests using a mocked Qdrant client."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from DBDuck.udom.adapters.vector_adapter import VectorAdapter


def _mock_point(pid: str, payload: dict, vector=None, score=None):
    return SimpleNamespace(id=pid, payload=payload, vector=vector, score=score)


def test_create_collection() -> None:
    client = MagicMock()
    adapter = VectorAdapter(db_instance="qdrant", client=client)
    adapter._models = SimpleNamespace(VectorParams=lambda size, distance: {"size": size, "distance": distance}, Distance=SimpleNamespace(COSINE="cosine", EUCLID="euclid", DOT="dot"))
    adapter._distance_map = {"cosine": "cosine", "euclid": "euclid", "dot": "dot"}
    result = adapter.create_collection("products", vector_size=3, distance="cosine")
    assert result["collection"] == "products"
    client.recreate_collection.assert_called_once()


def test_upsert_vector() -> None:
    client = MagicMock()
    adapter = VectorAdapter(db_instance="qdrant", client=client)
    adapter._models = SimpleNamespace(PointStruct=lambda id, vector, payload: {"id": id, "vector": vector, "payload": payload})
    result = adapter.upsert_vector("products", id="p1", vector=[0.1, 0.2], metadata={"name": "Widget"})
    assert result["id"] == "p1"
    client.upsert.assert_called_once()


def test_search_similar() -> None:
    client = MagicMock()
    client.search.return_value = [_mock_point("p1", {"name": "Widget"}, score=0.99)]
    adapter = VectorAdapter(db_instance="qdrant", client=client)
    results = adapter.search_similar("products", vector=[0.1, 0.2], top_k=5)
    assert results == [{"id": "p1", "score": 0.99, "metadata": {"name": "Widget"}}]


def test_delete_vector() -> None:
    client = MagicMock()
    adapter = VectorAdapter(db_instance="qdrant", client=client)
    adapter._models = SimpleNamespace(PointIdsList=lambda points: {"points": points})
    result = adapter.delete_vector("products", "p1")
    assert result == {"deleted": True, "id": "p1"}
    client.delete.assert_called_once()
