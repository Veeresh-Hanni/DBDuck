"""DBDuck command-line interface."""

from __future__ import annotations

import argparse
import json
import os
import subprocess  # nosec B404
import sys
import time
import traceback
from pathlib import Path
from typing import Any

from sqlalchemy import inspect as sa_inspect

from DBDuck import UDOM, __version__
from DBDuck.core.exceptions import QueryError

_HISTORY_FILE = Path.home() / ".dbduck_history"
_SUPPORTED_BACKENDS = {
    "sql": ["sqlite", "mysql", "postgres", "postgresql", "mssql", "sqlserver"],
    "nosql": ["mongodb", "mongo"],
    "graph": ["neo4j"],
    "vector": ["qdrant", "pinecone", "weaviate", "chroma"],
    "ai": ["openai", "azure-openai", "bedrock", "vertexai", "ollama"],
}


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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dbduck", description="DBDuck command-line interface")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ping = subparsers.add_parser("ping", help="Connect and ping the backend")
    ping.add_argument("--url", required=True)
    ping.add_argument("--type", required=True, dest="db_type")
    ping.add_argument("--instance", dest="db_instance")

    shell = subparsers.add_parser("shell", help="Interactive UQL shell")
    shell.add_argument("--url", required=True)
    shell.add_argument("--type", required=True, dest="db_type")
    shell.add_argument("--instance", dest="db_instance")
    shell.add_argument(
        "--debug-errors",
        action="store_true",
        help="Print the real exception and traceback for failed shell commands",
    )

    inspect_cmd = subparsers.add_parser("inspect", help="Inspect an entity schema or structure")
    inspect_cmd.add_argument("--url", required=True)
    inspect_cmd.add_argument("--type", required=True, dest="db_type")
    inspect_cmd.add_argument("--entity", required=True)
    inspect_cmd.add_argument("--instance", dest="db_instance")

    migrate = subparsers.add_parser("migrate", help="Run Alembic migrations for SQL backends")
    migrate.add_argument("--url", required=True)
    migrate.add_argument("--direction", required=True, choices=["up", "down"])
    migrate.add_argument("--revision", default="head")

    subparsers.add_parser("version", help="Print DBDuck version and supported backends")
    return parser


def _make_db(args: argparse.Namespace) -> UDOM:
    return UDOM(db_type=args.db_type, db_instance=args.db_instance, url=args.url)


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


def _inspect_entity(db: UDOM, db_type: str, entity: str) -> Any:
    if db_type == "sql":
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
        inspector = sa_inspect(db.adapter.engine)
        return sorted(inspector.get_table_names())
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
    _setup_readline()
    print("DBDuck shell. Enter UQL commands, or type 'exit'.")
    try:
        while True:
            try:
                line = input("dbduck> ").strip()
            except EOFError:
                print()
                break
            if not line:
                continue
            if line.lower() in {"exit", "quit"}:
                break
            try:
                result = _run_shell_command(db, args.db_type, line)
                print(_format_shell_result(line, result))
            except Exception as exc:
                print(f"error: {exc}")
                if getattr(args, "debug_errors", False):
                    print(f"debug-error: {_root_exception_message(exc)}", file=sys.stderr)
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
    finally:
        db.close()


def _cmd_migrate(args: argparse.Namespace) -> int:
    migrations_dir = Path("migrations") / "sql"
    alembic_ini = migrations_dir / "alembic.ini"
    if not alembic_ini.exists():
        raise SystemExit("Alembic configuration not found at migrations/sql/alembic.ini")
    revision = args.revision if args.direction == "up" else "-1"
    env = os.environ.copy()
    env["DBDUCK_DATABASE_URL"] = args.url
    command = [sys.executable, "-m", "alembic", "-c", str(alembic_ini), "upgrade" if args.direction == "up" else "downgrade", revision]
    completed = subprocess.run(command, check=False, env=env)  # nosec B603
    return int(completed.returncode)


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
    if args.command == "ping":
        return _cmd_ping(args)
    if args.command == "shell":
        return _cmd_shell(args)
    if args.command == "inspect":
        return _cmd_inspect(args)
    if args.command == "migrate":
        return _cmd_migrate(args)
    if args.command == "version":
        return _cmd_version()
    parser.error("Unknown command")
    return 1


if __name__ == "__main__":
    raise SystemExit(app())

