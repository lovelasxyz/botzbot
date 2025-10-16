"""Database migration utility for upgrading existing installations.

Run this script once after updating the bot to apply any schema changes
required for new features. The script is safe to run multiple times; it
tracks the applied version using SQLite's PRAGMA user_version flag.

Usage:
    python -m database.migration

This will read the database path from the bot configuration and apply
any pending migrations.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Callable, Iterable, Tuple

from loguru import logger

# Ensure project root is on sys.path when executed as a script
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from utils.config import Config  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for legacy deployments
    Config = None

Migration = Callable[[sqlite3.Connection], None]


def migration_add_schedule_table(conn: sqlite3.Connection) -> None:
    """Ensure the schedule table and related indexes exist."""
    logger.info("Ensuring schedule table exists")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(channel_id, start_time, end_time)
        );

        CREATE INDEX IF NOT EXISTS idx_schedule_times ON schedule(start_time, end_time);
        CREATE INDEX IF NOT EXISTS idx_schedule_channel ON schedule(channel_id);
        """
    )


def migration_add_chat_metadata_table(conn: sqlite3.Connection) -> None:
    """Add chat metadata table for storing chat titles/usernames."""
    logger.info("Ensuring chat_metadata table exists")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS chat_metadata (
            chat_id INTEGER PRIMARY KEY,
            title TEXT,
            username TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_chat_metadata_title ON chat_metadata(title);
        """
    )


MIGRATIONS: Iterable[Tuple[int, Migration]] = (
    (1, migration_add_schedule_table),
    (2, migration_add_chat_metadata_table),
)


def _get_db_path() -> Path:
    # Priority 1: explicit environment variable
    env_db_path = os.getenv("DB_PATH")
    if env_db_path:
        db_path = Path(env_db_path).expanduser().resolve()
        logger.info(f"Using database from DB_PATH env: {db_path}")
        return db_path

    # Priority 2: Config singleton from project code (if available)
    if Config is not None:
        try:
            config = Config()
            db_path = Path(config.db_path).expanduser().resolve()
            logger.info(f"Using database from Config: {db_path}")
            return db_path
        except Exception as exc:  # pragma: no cover - defensive fallback
            logger.warning(f"Failed to load Config, falling back to config file: {exc}")

    # Priority 3: read bot_config.json manually (legacy support)
    config_path = Path(os.getenv("BOT_CONFIG_PATH", "bot_config.json")).expanduser()
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as cfg:
                data = json.load(cfg)
            legacy_path = data.get("db_path") or data.get("database")
            if legacy_path:
                db_path = Path(legacy_path).expanduser().resolve()
                logger.info(f"Using database from {config_path}: {db_path}")
                return db_path
        except Exception as exc:  # pragma: no cover - defensive fallback
            logger.warning(f"Failed to read {config_path}: {exc}")

    # Final fallback: forwarder.db in project root
    fallback = ROOT_DIR / "forwarder.db"
    logger.warning(f"Falling back to default database path: {fallback}")
    return fallback.resolve()


def _apply_migrations(conn: sqlite3.Connection) -> None:
    current_version = conn.execute("PRAGMA user_version").fetchone()[0]
    logger.info(f"Current database version: {current_version}")

    for version, migration in MIGRATIONS:
        if current_version >= version:
            logger.debug(f"Skipping migration {version}, already applied")
            continue

        logger.info(f"Applying migration {version}: {migration.__name__}")
        migration(conn)
        conn.execute(f"PRAGMA user_version = {version}")
        conn.commit()
        logger.success(f"Migration {version} applied successfully")

    logger.info("Database is up to date")


def run() -> None:
    db_path = _get_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        _apply_migrations(conn)


if __name__ == "__main__":  # pragma: no cover
    try:
        run()
    except Exception as exc:
        logger.exception(f"Migration failed: {exc}")
        raise
