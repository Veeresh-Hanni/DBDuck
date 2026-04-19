from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

from DBDuck.alembic_support import apply_sqlalchemy_migration_compat, build_metadata_from_models, load_model_classes

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _prepare_project_import_path() -> None:
    project_dir = os.getenv("DBDUCK_PROJECT_DIR", "").strip()
    if not project_dir:
        return
    resolved = str(Path(project_dir).resolve())
    if resolved not in sys.path:
        sys.path.insert(0, resolved)


_prepare_project_import_path()


def _target_metadata():
    module_name = os.getenv("DBDUCK_MODEL_MODULE", "").strip()
    model_names_raw = os.getenv("DBDUCK_MODEL_NAMES", "").strip()
    if not module_name:
        return None
    model_names = [item.strip() for item in model_names_raw.split(",") if item.strip()] if model_names_raw else []
    model_classes = load_model_classes(module_name, model_names)
    return build_metadata_from_models(model_classes)


target_metadata = _target_metadata()


def _database_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("DBDUCK_DATABASE_URL") or config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError("DATABASE_URL or DBDUCK_DATABASE_URL is required for Alembic migrations")
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = _database_url()
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        apply_sqlalchemy_migration_compat(connection.dialect.name)
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()