"""DBDuck command-line interface."""

from __future__ import annotations

import argparse
import json
import os
import subprocess  # nosec B404
import sys
import time
import traceback
from importlib import resources
from pathlib import Path
from typing import Any

from colorama import Fore, Style, init as colorama_init
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.exc import SQLAlchemyError

from DBDuck import UDOM, __version__
from DBDuck.core.adapter_router import AdapterRouter
from DBDuck.core.exceptions import ConnectionError, QueryError, TransactionError

_HISTORY_FILE = Path.home() / ".dbduck_history"
_SUPPORTED_BACKENDS = {
    "sql": ["sqlite", "mysql", "postgresql", "mssql", "sqlserver"],
    "nosql": ["mongodb", "mongo"],
    "graph": ["neo4j"],
    "vector": ["qdrant"],
    # "ai": ["openai", "azure-openai", "bedrock", "vertexai", "ollama"],
}

colorama_init()


def _color(text: str, color: str) -> str:
    return f"{color}{text}{Style.RESET_ALL}"


def _print_error(message: str) -> None:
    print(_color(message, Fore.RED))


def _print_hint(message: str) -> None:
    print(_color(message, Fore.YELLOW))


def _print_success(message: str) -> None:
    print(_color(message, Fore.GREEN))


def _root_exception_message(exc: BaseException) -> str:
    current: BaseException = exc
    seen: set[int] = set()
    while True:
        cause = getattr(current, "__cause__", None) or getattr(current, "__context__", None)
        if cause is None or id(cause) in seen:
            break
        seen.add(id(cause))
        current = cause
    return str(current).strip() or current.__class__.__name__


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _migration_template_asset(filename: str):
    package_asset = resources.files("DBDuck.migrations.sql").joinpath(filename)
    if package_asset.exists():
        return package_asset

    repo_asset = _repo_root() / "DBDuck" / "migrations" / "sql" / filename
    if repo_asset.exists():
        return repo_asset

    return None


def _write_text_if_missing(path: Path, content: str) -> None:
    if not path.exists():
        path.write_text(content, encoding="utf-8")


def _ensure_project_migration_workspace(project_dir: Path) -> Path:
    project_root = project_dir.resolve()
    target_dir = project_root / "migrations" / "sql"
    versions_dir = target_dir / "versions"
    target_dir.mkdir(parents=True, exist_ok=True)
    versions_dir.mkdir(parents=True, exist_ok=True)

    template_files = ("script.py.mako", "README.md")
    for filename in template_files:
        src = _migration_template_asset(filename)
        dst = target_dir / filename
        if src is None:
            raise SystemExit(f"Migration template not found: {_repo_root() / 'migrations' / 'sql' / filename}")
        if not dst.exists():
            dst.write_bytes(src.read_bytes())

    env_src = _migration_template_asset("env.py")
    env_dst = target_dir / "env.py"
    if env_src is None:
        raise SystemExit(f"Migration template not found: {_repo_root() / 'migrations' / 'sql' / 'env.py'}")
    env_dst.write_bytes(env_src.read_bytes())

    _write_text_if_missing(
        target_dir / "alembic.ini",
        "\n".join(
            [
                "[alembic]",
                "script_location = migrations/sql",
                "prepend_sys_path = .",
                "version_locations = migrations/sql/versions",
                "sqlalchemy.url = sqlite:///test.db",
                "",
                "[loggers]",
                "keys = root,sqlalchemy,alembic",
                "",
                "[handlers]",
                "keys = console",
                "",
                "[formatters]",
                "keys = generic",
                "",
                "[logger_root]",
                "level = WARN",
                "handlers = console",
                "",
                "[logger_sqlalchemy]",
                "level = WARN",
                "handlers =",
                "qualname = sqlalchemy.engine",
                "",
                "[logger_alembic]",
                "level = INFO",
                "handlers =",
                "qualname = alembic",
                "",
                "[handler_console]",
                "class = StreamHandler",
                "args = (sys.stderr,)",
                "level = NOTSET",
                "formatter = generic",
                "",
                "[formatter_generic]",
                "format = %(levelname)-5.5s [%(name)s] %(message)s",
                "",
            ]
        ),
    )
    _write_text_if_missing(versions_dir / ".gitkeep", "")
    return target_dir


def _resolve_database_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("DBDUCK_DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL or DBDUCK_DATABASE_URL is required")
    return url


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dbduck", description="DBDuck command-line interface")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ping = subparsers.add_parser("ping", help="Connect and ping the backend")
    ping.add_argument("--type", dest="db_type")
    ping.add_argument("--instance", dest="db_instance")

    shell = subparsers.add_parser("shell", help="Interactive UQL shell")
    shell.add_argument("--type", dest="db_type")
    shell.add_argument("--instance", dest="db_instance")
    shell.add_argument(
        "--debug-errors",
        action="store_true",
        help="Print the real exception and traceback for failed shell commands",
    )

    inspect_cmd = subparsers.add_parser("inspect", help="Inspect an entity schema or structure")
    inspect_cmd.add_argument("--type", dest="db_type")
    inspect_cmd.add_argument("--entity", required=True)
    inspect_cmd.add_argument("--instance", dest="db_instance")

    makemigrations = subparsers.add_parser("makemigrations", help="Create an Alembic revision from UModel classes")
    makemigrations.add_argument(
        "--module",
        "--mod",
        dest="module",
        required=True,
        help="Import path containing UModel classes",
    )
    makemigrations.add_argument(
        "--message",
        "-m",
        dest="message",
        required=True,
        help="Alembic revision message",
    )
    makemigrations.add_argument(
        "--project-dir",
        default=os.getcwd(),
        help="Project root to add to Python imports before loading the model module (defaults to current directory)",
    )
    makemigrations.add_argument(
        "--model",
        dest="models",
        action="append",
        help="Limit autogeneration to a specific model class name; repeat to include more",
    )

    migrate = subparsers.add_parser("migrate", help="Run Alembic migrations for SQL backends")
    migrate.add_argument("--direction", required=True, choices=["up", "down"])
    migrate.add_argument("--revision", default="head")
    migrate.add_argument(
        "--project-dir",
        default=os.getcwd(),
        help="Project root used as the working directory for migrations (defaults to current directory)",
    )

    subparsers.add_parser("version", help="Print DBDuck version and supported backends")
    return parser


def _make_db(args: argparse.Namespace) -> UDOM:
    url = _resolve_database_url()
    db_type, db_instance = _resolve_backend_inputs(url, args.db_type, args.db_instance)
    args.db_type = db_type
    args.db_instance = db_instance
    log_level = os.getenv("DBDUCK_CLI_LOG_LEVEL")
    if not log_level:
        log_level = "DEBUG" if getattr(args, "debug_errors", False) else "CRITICAL"
    return UDOM(db_type=db_type, db_instance=db_instance, url=url, log_level=log_level)


def _infer_backend_from_url(url: str) -> tuple[str, str] | None:
    inferred_sql = AdapterRouter.infer_sql_instance_from_url(url)
    if inferred_sql:
        return "sql", inferred_sql
    lowered = (url or "").lower()
    if lowered.startswith(("mongodb://", "mongodb+srv://")):
        return "nosql", "mongodb"
    if lowered.startswith(("bolt://", "neo4j://")):
        return "graph", "neo4j"
    if ":6333" in lowered or "qdrant" in lowered:
        return "vector", "qdrant"
    return None


def _normalize_backend_alias(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    aliases = {
        "postgresql": "postgres",
        "postgres": "postgres",
        "psql": "postgres",
        "pg": "postgres",
        "mongo": "mongodb",
        "mongodb": "mongodb",
        "sqlserver": "mssql",
        "ms-sql": "mssql",
        "mssql": "mssql",
        "sqlite3": "sqlite",
    }
    return aliases.get(normalized, normalized)


def _resolve_backend_inputs(url: str, db_type: str | None, db_instance: str | None) -> tuple[str, str | None]:
    normalized_type = _normalize_backend_alias(db_type)
    normalized_instance = _normalize_backend_alias(db_instance)
    inferred = _infer_backend_from_url(url)

    if normalized_type in {"postgres", "sqlite", "mysql", "mssql"} and normalized_instance is None:
        normalized_instance = normalized_type
        normalized_type = "sql"
    if normalized_type in {"mongodb"} and normalized_instance is None:
        normalized_instance = normalized_type
        normalized_type = "nosql"
    if normalized_type in {"neo4j"} and normalized_instance is None:
        normalized_instance = normalized_type
        normalized_type = "graph"
    if normalized_type in {"qdrant"} and normalized_instance is None:
        normalized_instance = normalized_type
        normalized_type = "vector"

    if normalized_type is None:
        if inferred:
            return inferred
        return "sql", normalized_instance

    if normalized_instance is None and inferred and inferred[0] == normalized_type:
        normalized_instance = inferred[1]

    return normalized_type, normalized_instance


def _format_result(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, default=str)
    return str(value)


def _format_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "(no rows)"
    headers = list(rows[0].keys())
    widths = {header: len(header) for header in headers}
    normalized_rows: list[dict[str, str]] = []
    for row in rows:
        normalized: dict[str, str] = {}
        for header in headers:
            text = str(row.get(header, ""))
            normalized[header] = text
            widths[header] = max(widths[header], len(text))
        normalized_rows.append(normalized)
    header_line = "  ".join(header.ljust(widths[header]) for header in headers)
    separator_line = "  ".join("-" * widths[header] for header in headers)
    body_lines = [
        "  ".join(row[header].ljust(widths[header]) for header in headers) for row in normalized_rows
    ]
    return "\n".join([header_line, separator_line, *body_lines])


def _normalize_shell_line(line: str) -> str:
    return line.strip().rstrip(";").strip()


def _map_sqlalchemy_error(db: UDOM, exc: Exception) -> ConnectionError | QueryError:
    if hasattr(db.adapter, "_is_connection_error") and isinstance(exc, SQLAlchemyError):
        if db.adapter._is_connection_error(exc):
            return ConnectionError("Database connection failed")
    if hasattr(db.adapter, "_is_connection_like_exception") and db.adapter._is_connection_like_exception(exc):
        return ConnectionError("Database connection failed")
    if hasattr(db.adapter, "_clean_error_message"):
        return QueryError(db.adapter._clean_error_message(exc))
    return QueryError(str(exc) or exc.__class__.__name__)


def _friendly_error_detail(exc: BaseException) -> str | None:
    text = _root_exception_message(exc).lower()
    if "does not exist" in text and "database" in text:
        return "The target database does not exist yet."
    if "authentication failed" in text or "password authentication failed" in text or "login failed" in text:
        return "Authentication failed. Check the username/password in your URL."
    if "connection refused" in text or "could not connect" in text or "is the server running" in text:
        return "The database server is not reachable. Check host, port, and whether the server is running."
    if "could not translate host name" in text or "name or service not known" in text:
        return "The hostname in the URL could not be resolved."
    return None


def _extract_alembic_failure_message(stderr: str) -> str:
    lines = [line.strip() for line in (stderr or "").splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith(("Traceback", "File ")):
            continue
        if line.startswith(("INFO ", "Generating ")):
            continue
        if ": " in line and any(
            token in line
            for token in (
                "Error",
                "Exception",
                "FAILED",
                "ModuleNotFoundError",
                "OperationalError",
                "ProgrammingError",
                "QueryError",
            )
        ):
            return line
        if line.startswith("FAILED:"):
            return line
    return lines[-1] if lines else "Alembic command failed"


def _print_alembic_failure(stderr: str) -> None:
    message = _extract_alembic_failure_message(stderr)
    _print_error(f"error: {message}")
    lowered = message.lower()
    if "failed to import module" in lowered:
        _print_hint("hint:  Check --module, run from your project root, or pass --project-dir explicitly.")
        return
    if "target database is not up to date" in lowered:
        _print_hint("hint:  Apply existing migrations first with `dbduck migrate --direction up`, or use a fresh database.")
        return
    if "already an object named" in lowered or "already exists" in lowered:
        _print_hint("hint:  Schema already exists in this database. Use either Alembic or `python migrate.py`, not both on the same DB.")
        return
    detail = _friendly_error_detail(Exception(message))
    if detail:
        _print_hint(f"hint:  {detail}")


def _inspect_entity(db: UDOM, db_type: str, entity: str) -> Any:
    if db_type == "sql":
        try:
            inspector = sa_inspect(db.adapter.engine)
            columns = inspector.get_columns(entity)
            pk = inspector.get_pk_constraint(entity) or {}
            pk_columns = {str(name) for name in pk.get("constrained_columns") or []}
            unique_columns: set[str] = set()
            for constraint in inspector.get_unique_constraints(entity) or []:
                for column in constraint.get("column_names") or []:
                    unique_columns.add(str(column))
            enriched = []
            for column in columns:
                item = dict(column)
                name = str(item.get("name", ""))
                item["primary_key"] = name in pk_columns
                item["unique"] = name in unique_columns
                enriched.append(item)
            return enriched
        except Exception as exc:
            raise _map_sqlalchemy_error(db, exc) from exc
    if db_type == "nosql" and getattr(db.adapter, "db_instance", "") == "mongodb":
        db.adapter._ensure_mongo()
        sample = db.adapter._db[entity].find_one() or {}
        return {"fields": sorted(sample.keys())}
    if db_type == "graph":
        return db.find(entity, limit=1)
    if db_type == "vector":
        return db.collection_info(entity)
    raise QueryError("inspect is not supported for this backend")


def _list_entities(db: UDOM, db_type: str) -> Any:
    if db_type == "sql":
        try:
            inspector = sa_inspect(db.adapter.engine)
            return sorted(inspector.get_table_names())
        except Exception as exc:
            raise _map_sqlalchemy_error(db, exc) from exc
    if db_type == "nosql" and getattr(db.adapter, "db_instance", "") == "mongodb":
        db.adapter._ensure_mongo()
        return sorted(db.adapter._db.list_collection_names())
    raise QueryError("SHOW TABLES is currently supported for SQL and MongoDB shell sessions only")


def _run_shell_command(db: UDOM, db_type: str, line: str) -> Any:
    normalized = _normalize_shell_line(line)
    upper = normalized.upper()
    if upper == "SHOW TABLES":
        return _list_entities(db, db_type)
    if upper.startswith("SHOW SCHEMA "):
        entity = normalized[len("SHOW SCHEMA ") :].strip()
        if not entity:
            raise QueryError("SHOW SCHEMA requires an entity name")
        return _inspect_entity(db, db_type, entity)
    if upper.startswith("DESCRIBE "):
        entity = normalized[len("DESCRIBE ") :].strip()
        if not entity:
            raise QueryError("DESCRIBE requires an entity name")
        return _inspect_entity(db, db_type, entity)
    if upper == "HELP":
        return {
            "commands": [
                "FIND <entity> [WHERE ...] [ORDER BY ...] [LIMIT n]",
                "CREATE <entity> {field: value, ...}",
                "DELETE <entity> WHERE ...",
                "SHOW TABLES",
                "SHOW SCHEMA <entity>",
                "DESCRIBE <entity>",
                "exit",
            ]
        }
    return db.uexecute(normalized)


def _format_shell_result(command: str, result: Any) -> str:
    normalized = _normalize_shell_line(command).upper()
    if normalized.startswith("DESCRIBE ") or normalized.startswith("SHOW SCHEMA "):
        if isinstance(result, list) and all(isinstance(item, dict) for item in result):
            table_rows = [{str(key): value for key, value in item.items()} for item in result]
            return _format_table(table_rows)
    return _format_result(result)


def _setup_readline() -> None:
    try:
        import readline
    except Exception:
        return
    commands = ["FIND ", "CREATE ", "DELETE ", "UPDATE ", "SHOW TABLES", "SHOW SCHEMA ", "DESCRIBE ", "HELP"]

    def _complete(text: str, state: int):
        matches = [command for command in commands if command.startswith(text.upper())]
        return matches[state] if state < len(matches) else None

    try:
        readline.read_history_file(_HISTORY_FILE)
    except FileNotFoundError:
        pass
    readline.set_completer(_complete)
    readline.parse_and_bind("tab: complete")


def _save_history() -> None:
    try:
        import readline
    except Exception:
        return
    try:
        readline.write_history_file(_HISTORY_FILE)
    except Exception:
        return


def _cmd_ping(args: argparse.Namespace) -> int:
    started = time.perf_counter()
    db = _make_db(args)
    try:
        result = db.ping()
        latency_ms = (time.perf_counter() - started) * 1000.0
        version_info = None
        if args.db_type == "sql":
            try:
                rows = db.execute("SELECT 1")
                version_info = getattr(db.adapter, "DIALECT", db.db_instance)
                if isinstance(rows, list) and rows:
                    version_info = f"{version_info} (connected)"
            except Exception:
                version_info = getattr(db.adapter, "DIALECT", db.db_instance)
        elif args.db_type == "graph":
            version_info = "neo4j"
        elif args.db_type == "vector":
            version_info = db.db_instance
        else:
            version_info = db.db_instance
        print(_format_result({"ok": True, "latency_ms": round(latency_ms, 2), "backend": version_info, "ping": result}))
        return 0
    finally:
        db.close()


def _cmd_shell(args: argparse.Namespace) -> int:
    db = _make_db(args)
    db.ping()
    _setup_readline()
    _print_success("DBDuck shell. Enter UQL commands, or type 'exit'.")
    try:
        while True:
            try:
                line = input("dbduck> ").strip()
                if not line:
                    continue
                if line.lower() in {"exit", "quit"}:
                    break
                result = _run_shell_command(db, args.db_type, line)
                print(_format_shell_result(line, result))
            except ConnectionError as exc:
                _print_error(f"error: Cannot reach database - {exc}")
                detail = _friendly_error_detail(exc)
                if detail:
                    _print_hint(f"hint:  {detail}")
                _print_hint("hint:  Check DATABASE_URL or DBDUCK_DATABASE_URL in your environment.")
            except QueryError as exc:
                msg = str(exc)
                _print_error(f"error: {msg}")
                if getattr(args, "debug_errors", False):
                    print(f"debug-error: {exc!r}", file=sys.stderr)
                    traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
                first_word = line.strip().split()[0].upper() if line.strip() else ""
                known = {"FIND", "CREATE", "DELETE", "UPDATE", "COUNT", "SHOW", "DESCRIBE", "HELP", "PING", "VERSION", "HISTORY", "CLEAR", "EXPORT"}
                if first_word not in known and first_word:
                    closest = min(known, key=lambda k: abs(len(k) - len(first_word)))
                    _print_hint(f"hint:  Unknown command '{first_word}'. Did you mean: {closest}? Type HELP for all commands.")
                elif "injection" in msg.lower():
                    _print_hint("security: Blocked potential injection attempt. Event logged to security_logs.")
                elif "not found" in msg.lower() or "execution failed" in msg.lower():
                    _print_hint("hint:  Use SHOW TABLES to see available tables.")
            except TransactionError as exc:
                _print_error(f"error: Transaction failed - {exc}")
            except KeyboardInterrupt:
                _print_hint("\nUse 'exit' to quit the shell.")
    except Exception as exc:
        _print_error(f"error: {exc}")
        if getattr(args, "debug_errors", False):
            traceback.print_exception(type(exc), exc, exc.__traceback__)
    finally:
        _save_history()
        db.close()
    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    db = _make_db(args)
    entity = args.entity
    try:
        print(_format_shell_result(f"DESCRIBE {entity}", _inspect_entity(db, args.db_type, entity)))
        return 0
    except ConnectionError as exc:
        _print_error(f"error: Cannot reach database - {exc}")
        detail = _friendly_error_detail(exc)
        if detail:
            _print_hint(f"hint:  {detail}")
        return 1
    except QueryError as exc:
        _print_error(f"error: {exc}")
        return 1
    finally:
        db.close()


def _run_alembic_command(*, command: list[str], env: dict[str, str]) -> int:
    project_dir = Path(env.get("DBDUCK_PROJECT_DIR", str(_repo_root())))
    migrations_dir = _ensure_project_migration_workspace(project_dir)
    alembic_ini = migrations_dir / "alembic.ini"
    if not alembic_ini.exists():
        raise SystemExit("Alembic configuration not found at migrations/sql/alembic.ini")
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(alembic_ini), *command],
        check=False,
        env=env,
        cwd=str(project_dir.resolve()),
        capture_output=True,
        text=True,
    )  # nosec B603
    stdout = (getattr(completed, "stdout", "") or "").strip()
    stderr = (getattr(completed, "stderr", "") or "").strip()
    if completed.returncode == 0:
        if stdout:
            print(stdout)
        if stderr:
            for line in stderr.splitlines():
                clean = line.strip()
                if not clean:
                    continue
                if clean.startswith("INFO "):
                    print(_color(clean, Fore.CYAN))
                elif clean.startswith("Generating "):
                    _print_success(clean)
                else:
                    print(clean)
    else:
        _print_alembic_failure(stderr or stdout)
    return int(completed.returncode)


def _cmd_makemigrations(args: argparse.Namespace) -> int:
    url = _resolve_database_url()
    env = os.environ.copy()
    env["DATABASE_URL"] = url
    env["DBDUCK_DATABASE_URL"] = url
    env["DBDUCK_MODEL_MODULE"] = args.module
    env["DBDUCK_PROJECT_DIR"] = str(Path(args.project_dir).resolve())
    if args.models:
        env["DBDUCK_MODEL_NAMES"] = ",".join(args.models)
    else:
        env.pop("DBDUCK_MODEL_NAMES", None)
    exit_code = _run_alembic_command(
        command=["revision", "--autogenerate", "-m", args.message],
        env=env,
    )
    if exit_code == 0:
        _print_success("makemigrations completed")
        print(
            _format_result(
                {
                    "status": "revision_created",
                    "module": args.module,
                    "message": args.message,
                    "models": args.models or [],
                }
            )
        )
    return exit_code


def _cmd_migrate(args: argparse.Namespace) -> int:
    revision = args.revision if args.direction == "up" else "-1"
    env = os.environ.copy()
    url = _resolve_database_url()
    env["DATABASE_URL"] = url
    env["DBDUCK_DATABASE_URL"] = url
    env["DBDUCK_PROJECT_DIR"] = str(Path(args.project_dir).resolve())
    exit_code = _run_alembic_command(
        command=["upgrade" if args.direction == "up" else "downgrade", revision],
        env=env,
    )
    if exit_code == 0:
        _print_success("migration completed")
    return exit_code


def _cmd_version() -> int:
    print(
        _format_result(
            {
                "version": __version__,
                "supported_backends": _SUPPORTED_BACKENDS,
            }
        )
    )
    return 0


def app(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "ping":
            return _cmd_ping(args)
        if args.command == "shell":
            return _cmd_shell(args)
        if args.command == "inspect":
            return _cmd_inspect(args)
        if args.command == "makemigrations":
            return _cmd_makemigrations(args)
        if args.command == "migrate":
            return _cmd_migrate(args)
        if args.command == "version":
            return _cmd_version()
        parser.error("Unknown command")
        return 1
    except ConnectionError as exc:
        _print_error(f"error: Cannot reach database - {exc}")
        detail = _friendly_error_detail(exc)
        if detail:
            _print_hint(f"hint:  {detail}")
        return 1
    except QueryError as exc:
        _print_error(f"error: {exc}")
        return 1
    except TransactionError as exc:
        _print_error(f"error: Transaction failed - {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(app())
