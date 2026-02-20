from __future__ import annotations

from DBDuck.core.connection_manager import ConnectionManager


def test_parse_url_sqlite() -> None:
    parsed = ConnectionManager.parse_url("sqlite:///tmp_test.db")
    assert parsed.dialect == "sqlite"
    assert parsed.database == "tmp_test.db"


def test_engine_is_cached_per_url(tmp_path) -> None:
    db_path = tmp_path / "cache.db"
    url = f"sqlite:///{db_path.as_posix()}"
    manager = ConnectionManager()
    e1 = manager.get_engine(url)
    e2 = manager.get_engine(url)
    assert e1 is e2
