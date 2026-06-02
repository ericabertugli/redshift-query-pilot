#!/usr/bin/env python3
"""Sync table/column descriptions from knowledge YAML files into catalog.db.

All files in knowledge/ must follow a single YAML format (see knowledge/README.md).
Multiple files can describe the same table/column — descriptions are stored per
source file and presented together with attribution.
"""

import argparse
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

DB_PATH = os.environ.get("CATALOG_DB_PATH", Path(__file__).parent / "catalog.db")
KNOWLEDGE_DIR = Path(__file__).parent / "knowledge"

KNOWLEDGE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS table_descriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    database_name TEXT NOT NULL DEFAULT '',
    table_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    description TEXT NOT NULL,
    last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(database_name, table_name, source_file)
);

CREATE TABLE IF NOT EXISTS column_descriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    database_name TEXT NOT NULL DEFAULT '',
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    description TEXT NOT NULL,
    last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(database_name, table_name, column_name, source_file)
);

CREATE INDEX IF NOT EXISTS idx_table_desc_lookup
    ON table_descriptions(table_name);
CREATE INDEX IF NOT EXISTS idx_col_desc_lookup
    ON column_descriptions(table_name, column_name);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_yaml_file(
    path: Path, rel_path: str
) -> tuple[list[tuple], list[tuple]]:
    """Parse a single knowledge YAML file.

    Returns (table_rows, column_rows) where each row is a tuple ready for DB insert.
    """
    try:
        content = yaml.safe_load(path.read_text())
    except (yaml.YAMLError, OSError) as exc:
        logger.warning("Skipping %s: %s", rel_path, exc)
        return [], []

    if not isinstance(content, dict) or "tables" not in content:
        logger.debug("Skipping %s: no 'tables' key", rel_path)
        return [], []

    table_rows = []
    col_rows = []

    for table in content["tables"]:
        if not isinstance(table, dict):
            continue
        table_name = table.get("name") or ""
        if not table_name:
            continue

        db_name = table.get("database") or ""
        table_desc = (table.get("description") or "").strip()

        if table_desc:
            table_rows.append((db_name, table_name, rel_path, table_desc))

        for col in table.get("columns") or []:
            if not isinstance(col, dict):
                continue
            col_name = col.get("name") or ""
            col_desc = (col.get("description") or "").strip()
            if col_name and col_desc:
                col_rows.append((db_name, table_name, col_name, rel_path, col_desc))

    return table_rows, col_rows


def sync_knowledge(db_path: str = str(DB_PATH)) -> tuple[int, int]:
    """Parse all YAML files in knowledge/ and upsert descriptions into catalog.db."""
    if not KNOWLEDGE_DIR.exists():
        logger.info("No knowledge/ directory found, nothing to sync.")
        return 0, 0

    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.executescript(KNOWLEDGE_SCHEMA_SQL)

    now = _now_iso()
    table_count = 0
    col_count = 0

    for yml_file in sorted(KNOWLEDGE_DIR.glob("*.yml")):
        rel_path = yml_file.name
        table_rows, col_rows = _parse_yaml_file(yml_file, rel_path)

        for db_name, table_name, src_file, desc in table_rows:
            conn.execute(
                """INSERT INTO table_descriptions
                   (database_name, table_name, source_file, description, last_synced)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(table_name, source_file)
                   DO UPDATE SET database_name=excluded.database_name,
                                 description=excluded.description,
                                 last_synced=excluded.last_synced""",
                (db_name, table_name, src_file, desc, now),
            )
            table_count += 1

        for db_name, table_name, col_name, src_file, desc in col_rows:
            conn.execute(
                """INSERT INTO column_descriptions
                   (database_name, table_name, column_name, source_file, description, last_synced)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(table_name, column_name, source_file)
                   DO UPDATE SET database_name=excluded.database_name,
                                 description=excluded.description,
                                 last_synced=excluded.last_synced""",
                (db_name, table_name, col_name, src_file, desc, now),
            )
            col_count += 1

    conn.commit()
    conn.close()
    logger.info("Synced %d table descriptions, %d column descriptions.", table_count, col_count)
    return table_count, col_count


def main():
    parser = argparse.ArgumentParser(description="Sync knowledge YAML files into catalog.db")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to catalog.db")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    t, c = sync_knowledge(args.db)
    logger.info("Done. Total: %d table descriptions, %d column descriptions.", t, c)


if __name__ == "__main__":
    main()
