from __future__ import annotations

import logging
from io import StringIO

import pytest

from DBDuck.adapters.sqlite_adapter import SQLiteAdapter
from DBDuck.core.exceptions import QueryError
from DBDuck.utils.logger import get_logger


def test_sqlalchemy_reference_url_is_removed_from_error_message() -> None:
    adapter = SQLiteAdapter(url="sqlite:///:memory:")
    message = adapter._clean_error_message(
        Exception(
            '(pymysql.err.OperationalError) (1049, "Unknown database \'dbduck\'") '
            "(Background on this error at: https://sqlalche.me/e/20/e3q8)"
        )
    )
    assert "sqlalche.me" not in message
    assert "Background on this error" not in message
    assert "Unknown database 'dbduck'" in message


def test_public_sql_errors_are_masked_and_internal_debug_is_logged(tmp_path) -> None:
    adapter = SQLiteAdapter(url=f"sqlite:///{(tmp_path / 'masked_errors.db').as_posix()}", log_level="DEBUG")
    logger = get_logger("DEBUG")
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    class _Conn:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("driver exploded: SELECT * FROM users WHERE password='secret'")

    adapter._active_connection = lambda: _Conn()  # type: ignore[method-assign]

    try:
        with pytest.raises(QueryError):
            adapter.run_native("SELECT 1")
    finally:
        logger.removeHandler(handler)

    logged = stream.getvalue()
    assert "driver exploded" in logged
    assert "Internal SQL execution failure" in logged
