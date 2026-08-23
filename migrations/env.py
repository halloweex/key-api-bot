"""Alembic entry point.

Thin, and without a model registry on purpose: revisions here are written by
hand against Postgres, so there is nothing to autogenerate from and no
metadata to import. That also keeps this file out of the application's import
graph — it reaches for `core.pg` only for two constants, and for nothing that
reads configuration or opens a connection of its own.

**Async, over asyncpg.** The application's driver is asyncpg and adding a
second one for migrations would mean two drivers' quirks to know for one
database. SQLAlchemy is here anyway as Alembic's own dependency.

**The version table lives in `meta`, named explicitly.** Left to the role's
`search_path` it would move the day the search_path changes, which is the exact
failure #120 fixed for the application's DDL. `create_schema` is off: the
platform owns schemas, this owns tables (charter rule 11), and `meta` is
created by `postgres/initdb/10-roles-and-schemas.sql`.
"""
from __future__ import annotations

import asyncio
import os

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from core.pg import DSN_ENV, VERSION_TABLE, VERSION_TABLE_SCHEMA

config = context.config

_dsn = os.getenv(DSN_ENV, "").strip()
if _dsn:
    # asyncpg needs the driver named in the URL; a DSN written for psql or for
    # asyncpg.connect() carries no driver at all, so accept both spellings.
    if _dsn.startswith("postgresql://"):
        _dsn = _dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif _dsn.startswith("postgres://"):
        _dsn = _dsn.replace("postgres://", "postgresql+asyncpg://", 1)
    # Escaped: a password containing '%' would otherwise be read as
    # configparser interpolation and truncate the URL.
    config.set_main_option("sqlalchemy.url", _dsn.replace("%", "%%"))


def _configure(**kwargs) -> None:
    context.configure(
        target_metadata=None,
        version_table=VERSION_TABLE,
        version_table_schema=VERSION_TABLE_SCHEMA,
        **kwargs,
    )


def run_migrations_offline() -> None:
    """Render the DDL as SQL without connecting.

    How a revision is reviewed before any database exists, and how CI can
    check that it compiles: `alembic upgrade head --sql`.
    """
    _configure(
        url=config.get_main_option("sqlalchemy.url")
        or "postgresql+asyncpg://localhost/unused",
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        dialect_name="postgresql",
    )
    with context.begin_transaction():
        context.run_migrations()


def _run(connection: Connection) -> None:
    _configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    if not config.get_main_option("sqlalchemy.url"):
        raise SystemExit(f"{DSN_ENV} is not set — refusing to guess a database")

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(_run)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
