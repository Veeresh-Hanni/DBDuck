"""CLI tests for the interactive DBDuck shell."""

from __future__ import annotations

import builtins

from DBDuck.cli.main import app


def test_shell_prints_banner_and_prompt(monkeypatch, capsys, tmp_path) -> None:
    prompts: list[str] = []

    def _fake_input(prompt: str) -> str:
        prompts.append(prompt)
        return "exit"

    monkeypatch.setattr(builtins, "input", _fake_input)

    exit_code = app(
        [
            "shell",
            "--url",
            f"sqlite:///{(tmp_path / 'cli.db').as_posix()}",
            "--type",
            "sql",
            "--instance",
            "sqlite",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "DBDuck shell. Enter UQL commands, or type 'exit'." in captured.out
    assert prompts == ["dbduck> "]


def test_shell_debug_errors_prints_traceback(monkeypatch, capsys, tmp_path) -> None:
    prompts = iter(["FIND missing_table", "exit"])

    def _fake_input(prompt: str) -> str:
        return next(prompts)

    monkeypatch.setattr(builtins, "input", _fake_input)

    exit_code = app(
        [
            "shell",
            "--url",
            f"sqlite:///{(tmp_path / 'cli.db').as_posix()}",
            "--type",
            "sql",
            "--instance",
            "sqlite",
            "--debug-errors",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "error: Database execution failed" in captured.out
    assert "debug-error:" in captured.err
    assert "Traceback" in captured.err


def test_shell_show_tables(monkeypatch, capsys, tmp_path) -> None:
    db_path = tmp_path / "cli_show_tables.db"
    from DBDuck import UDOM

    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_path.as_posix()}")
    try:
        db.create("users", {"id": 1, "name": "alice"})
    finally:
        db.close()

    prompts = iter(["SHOW TABLES", "exit"])

    def _fake_input(prompt: str) -> str:
        return next(prompts)

    monkeypatch.setattr(builtins, "input", _fake_input)

    exit_code = app(
        [
            "shell",
            "--url",
            f"sqlite:///{db_path.as_posix()}",
            "--type",
            "sql",
            "--instance",
            "sqlite",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert '"users"' in captured.out


def test_shell_describe(monkeypatch, capsys, tmp_path) -> None:
    db_path = tmp_path / "cli_describe.db"
    from DBDuck import UDOM

    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_path.as_posix()}")
    try:
        db.create("users", {"id": 1, "name": "alice"})
    finally:
        db.close()

    prompts = iter(["DESCRIBE users", "exit"])

    def _fake_input(prompt: str) -> str:
        return next(prompts)

    monkeypatch.setattr(builtins, "input", _fake_input)

    exit_code = app(
        [
            "shell",
            "--url",
            f"sqlite:///{db_path.as_posix()}",
            "--type",
            "sql",
            "--instance",
            "sqlite",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "name" in captured.out
    assert "type" in captured.out
    assert "primary_key" in captured.out
    assert "unique" in captured.out
    assert "id" in captured.out
    assert "name" in captured.out


def test_shell_show_schema(monkeypatch, capsys, tmp_path) -> None:
    db_path = tmp_path / "cli_show_schema.db"
    from DBDuck import UDOM

    db = UDOM(db_type="sql", db_instance="sqlite", url=f"sqlite:///{db_path.as_posix()}")
    try:
        db.create("users", {"id": 1, "name": "alice"})
    finally:
        db.close()

    prompts = iter(["SHOW SCHEMA users", "exit"])

    def _fake_input(prompt: str) -> str:
        return next(prompts)

    monkeypatch.setattr(builtins, "input", _fake_input)

    exit_code = app(
        [
            "shell",
            "--url",
            f"sqlite:///{db_path.as_posix()}",
            "--type",
            "sql",
            "--instance",
            "sqlite",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "name" in captured.out
    assert "type" in captured.out
    assert "primary_key" in captured.out
    assert "unique" in captured.out
    assert "id" in captured.out
    assert "name" in captured.out
