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

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{(tmp_path / 'cli.db').as_posix()}")
    exit_code = app(["shell", "--type", "sql", "--instance", "sqlite"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "DBDuck shell. Enter UQL commands, or type 'exit'." in captured.out
    assert prompts == ["dbduck> "]


def test_shell_debug_errors_prints_traceback(monkeypatch, capsys, tmp_path) -> None:
    prompts = iter(["FIND missing_table", "exit"])

    def _fake_input(prompt: str) -> str:
        return next(prompts)

    monkeypatch.setattr(builtins, "input", _fake_input)

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{(tmp_path / 'cli.db').as_posix()}")
    exit_code = app(["shell", "--type", "sql", "--instance", "sqlite", "--debug-errors"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "error: (sqlite3.OperationalError) no such table: missing_table" in captured.out
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

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    exit_code = app(["shell", "--type", "sql", "--instance", "sqlite"])

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

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    exit_code = app(["shell", "--type", "sql", "--instance", "sqlite"])

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

    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    exit_code = app(["shell", "--type", "sql", "--instance", "sqlite"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "name" in captured.out
    assert "type" in captured.out
    assert "primary_key" in captured.out
    assert "unique" in captured.out
    assert "id" in captured.out
    assert "name" in captured.out


def test_makemigrations_command_passes_model_module_to_alembic(monkeypatch, tmp_path, capsys) -> None:
    models_file = tmp_path / "temp_models_make.py"
    models_file.write_text(
        "\n".join(
            [
                "from DBDuck.models import UModel, Column, Integer, String",
                "",
                "class User(UModel):",
                "    class Meta:",
                "        db_table = 'users'",
                "",
                "    id = Column(Integer, primary_key=True)",
                "    name = Column(String, nullable=False)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{(tmp_path / 'cli_make.db').as_posix()}")
    recorded: dict[str, object] = {}

    class _Completed:
        returncode = 0

    def _fake_run(command, check=False, env=None, cwd=None, **kwargs):
        recorded["command"] = command
        recorded["env"] = env
        recorded["cwd"] = cwd
        return _Completed()

    monkeypatch.setattr("DBDuck.cli.main.subprocess.run", _fake_run)
    exit_code = app(
        [
            "makemigrations",
            "--mod",
            "temp_models_make",
            "--model",
            "User",
            "-m",
            "init-users",
        ]
    )

    captured_out = capsys.readouterr()
    assert exit_code == 0
    command = recorded["command"]
    env = recorded["env"]
    assert "revision" in command
    assert "--autogenerate" in command
    assert env["DATABASE_URL"] == f"sqlite:///{(tmp_path / 'cli_make.db').as_posix()}"
    assert env["DBDUCK_DATABASE_URL"] == f"sqlite:///{(tmp_path / 'cli_make.db').as_posix()}"
    assert env["DBDUCK_MODEL_MODULE"] == "temp_models_make"
    assert env["DBDUCK_MODEL_NAMES"] == "User"
    assert env["DBDUCK_PROJECT_DIR"] == str(tmp_path.resolve())


def test_makemigrations_long_and_short_flags_share_destinations(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{(tmp_path / 'cli_make.db').as_posix()}")

    captured: dict[str, object] = {}

    class _Completed:
        returncode = 0
        stdout = ""
        stderr = ""

    def _fake_run(cmd, cwd=None, env=None, **kwargs):
        captured["cmd"] = cmd
        captured["cwd"] = cwd
        captured["env"] = env
        return _Completed()

    monkeypatch.setattr("DBDuck.cli.main.subprocess.run", _fake_run)

    exit_code = app(["makemigrations", "--module", "models", "--message", "init"])

    assert exit_code == 0
    env = captured["env"]
    assert env["DBDUCK_MODEL_MODULE"] == "models"
    assert captured["cwd"] == str(tmp_path.resolve())


def test_migrate_command_uses_database_url_env(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _Completed:
        returncode = 0

    def _fake_run(command, check=False, env=None, cwd=None, **kwargs):
        captured["command"] = command
        captured["env"] = env
        captured["cwd"] = cwd
        return _Completed()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///env_cli_migrate.db")
    monkeypatch.setattr("DBDuck.cli.main.subprocess.run", _fake_run)
    exit_code = app(["migrate", "--direction", "up"])
    assert exit_code == 0
    env = captured["env"]
    assert env["DATABASE_URL"] == "sqlite:///env_cli_migrate.db"
    assert env["DBDUCK_DATABASE_URL"] == "sqlite:///env_cli_migrate.db"
    assert env["DBDUCK_PROJECT_DIR"] == str(tmp_path.resolve())
    assert captured["cwd"] == str(tmp_path.resolve())
    assert (tmp_path / "migrations" / "sql" / "alembic.ini").exists()
    assert (tmp_path / "migrations" / "sql" / "versions").is_dir()


def test_makemigrations_import_failure_is_rendered_as_colored_cli_error(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{(tmp_path / 'cli_make.db').as_posix()}")

    class _Completed:
        returncode = 1
        stdout = ""
        stderr = (
            "Traceback (most recent call last):\n"
            "ModuleNotFoundError: Failed to import module 'modelsd'. "
            "Run the command from your project root or pass --project-dir explicitly.\n"
        )

    def _fake_run(command, check=False, env=None, cwd=None, **kwargs):
        return _Completed()

    monkeypatch.setattr("DBDuck.cli.main.subprocess.run", _fake_run)
    exit_code = app(["makemigrations", "--module", "modelsd", "--message", "init"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "error:" in captured.out
    assert "Failed to import module 'modelsd'" in captured.out
    assert "hint:" in captured.out


def test_ping_command_uses_database_url_env(monkeypatch, capsys, tmp_path) -> None:
    db_url = f"sqlite:///{(tmp_path / 'cli_env_ping.db').as_posix()}"
    monkeypatch.setenv("DATABASE_URL", db_url)
    exit_code = app(["ping"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert '"ok": true' in captured.out.lower()
