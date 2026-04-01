#!/usr/bin/env python3
"""Sync table schemas from AWS Glue and Redshift into a local SQLite database."""

import argparse
import logging
import signal
import sqlite3

import boto3
import redshift_connector


class SamlAuthTimeout(Exception):
    """Raised when SAML authentication times out."""
    pass

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    table_type TEXT,
    location TEXT,
    storage_format TEXT,
    description TEXT,
    last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, database_name, table_name)
);

CREATE TABLE IF NOT EXISTS columns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    data_type TEXT NOT NULL,
    ordinal_position INTEGER,
    is_partition_key BOOLEAN DEFAULT 0,
    comment TEXT,
    FOREIGN KEY (table_id) REFERENCES tables(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS schema_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    glue_database TEXT NOT NULL,
    redshift_schema TEXT NOT NULL,
    catalog_name TEXT,
    region TEXT,
    last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(glue_database, redshift_schema)
);

CREATE INDEX IF NOT EXISTS idx_tables_lookup ON tables(database_name, table_name);
CREATE INDEX IF NOT EXISTS idx_columns_table ON columns(table_id);
CREATE INDEX IF NOT EXISTS idx_schema_mappings_glue ON schema_mappings(glue_database);
"""


def detect_storage_format(table_def):
    """Map Glue StorageDescriptor SerdeInfo/InputFormat to a human-readable format."""
    sd = table_def.get("StorageDescriptor", {})
    input_format = sd.get("InputFormat", "")
    serde = sd.get("SerdeInfo", {}).get("SerializationLibrary", "")

    if "parquet" in input_format.lower() or "parquet" in serde.lower():
        return "Parquet"
    if "orc" in input_format.lower() or "orc" in serde.lower():
        return "ORC"
    if "avro" in serde.lower():
        return "Avro"
    if "jsonserde" in serde.lower() or "json" in serde.lower():
        return "JSON"
    if "TextInputFormat" in input_format:
        if "csv" in serde.lower() or "OpenCSV" in serde or "lazy" in serde.lower():
            return "CSV"
        return "Text"
    if input_format or serde:
        return input_format.split(".")[-1] if input_format else serde.split(".")[-1]
    return None


def init_db(db_path):
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_SQL)
    return conn


def sync_glue(conn, database):
    """Sync all table schemas from AWS Glue."""
    logger.info("Starting Glue sync for database: %s", database)
    client = boto3.client("glue")

    tables_data = []
    paginator = client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        for table_def in page["TableList"]:
            try:
                sd = table_def.get("StorageDescriptor", {})
                table_info = {
                    "name": table_def["Name"],
                    "table_type": table_def.get("TableType"),
                    "location": sd.get("Location"),
                    "storage_format": detect_storage_format(table_def),
                    "description": table_def.get("Description"),
                    "columns": [],
                }

                for i, col in enumerate(sd.get("Columns", []), start=1):
                    table_info["columns"].append({
                        "name": col["Name"],
                        "type": col["Type"],
                        "position": i,
                        "is_partition": False,
                        "comment": col.get("Comment"),
                    })

                partition_keys = table_def.get("PartitionKeys", [])
                offset = len(table_info["columns"])
                for i, pk in enumerate(partition_keys, start=1):
                    table_info["columns"].append({
                        "name": pk["Name"],
                        "type": pk["Type"],
                        "position": offset + i,
                        "is_partition": True,
                        "comment": pk.get("Comment"),
                    })

                tables_data.append(table_info)
                logger.debug("Fetched Glue table: %s.%s", database, table_info["name"])
            except Exception:
                logger.warning(
                    "Failed to process Glue table: %s",
                    table_def.get("Name", "<unknown>"),
                    exc_info=True,
                )

    cursor = conn.cursor()
    cursor.execute("DELETE FROM columns WHERE table_id IN (SELECT id FROM tables WHERE source = 'glue')")
    cursor.execute("DELETE FROM tables WHERE source = 'glue'")

    for table_info in tables_data:
        cursor.execute(
            """INSERT INTO tables (source, database_name, table_name, table_type, location, storage_format, description)
               VALUES ('glue', ?, ?, ?, ?, ?, ?)""",
            (database, table_info["name"], table_info["table_type"],
             table_info["location"], table_info["storage_format"], table_info["description"]),
        )
        table_id = cursor.lastrowid
        for col in table_info["columns"]:
            cursor.execute(
                """INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key, comment)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (table_id, col["name"], col["type"], col["position"], col["is_partition"], col["comment"]),
            )

    conn.commit()
    logger.info("Glue sync complete: %d tables", len(tables_data))


def sync_redshift(conn, host, cluster, database, db_user, region, login_url, auth_timeout=120):
    """Sync all table schemas from Redshift."""
    logger.info("Starting Redshift sync for cluster=%s database=%s user=%s", cluster, database, db_user)

    def timeout_handler(signum, frame):
        raise SamlAuthTimeout(f"SAML authentication timed out after {auth_timeout} seconds")

    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(auth_timeout)
    try:
        rs_conn = redshift_connector.connect(
            iam=True,
            host=host,
            port=5439,
            cluster_identifier=cluster,
            database=database,
            db_user=db_user,
            region=region,
            credentials_provider="BrowserSamlCredentialsProvider",
            login_url=login_url,
        )
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

    try:
        cursor = rs_conn.cursor()

        cursor.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal', 'pg_automv')
            ORDER BY table_schema, table_name
        """)
        rs_tables = cursor.fetchall()

        cursor.execute("""
            SELECT table_schema, table_name, column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_internal', 'pg_automv')
            ORDER BY table_schema, table_name, ordinal_position
        """)
        rs_columns = cursor.fetchall()

        # Fetch external schema mappings (Glue database → Redshift schema)
        cursor.execute("""
            SELECT schemaname, databasename, esoptions
            FROM svv_external_schemas
            ORDER BY schemaname
        """)
        rs_schema_mappings = cursor.fetchall()
    finally:
        rs_conn.close()

    columns_by_table = {}
    for schema, table, col_name, dtype, pos in rs_columns:
        columns_by_table.setdefault((schema, table), []).append({
            "name": col_name,
            "type": dtype,
            "position": pos,
        })

    db_cursor = conn.cursor()
    db_cursor.execute("DELETE FROM columns WHERE table_id IN (SELECT id FROM tables WHERE source = 'redshift')")
    db_cursor.execute("DELETE FROM tables WHERE source = 'redshift'")

    count = 0
    for schema, table_name, table_type in rs_tables:
        try:
            db_cursor.execute(
                """INSERT INTO tables (source, database_name, table_name, table_type)
                   VALUES ('redshift', ?, ?, ?)""",
                (schema, table_name, table_type),
            )
            table_id = db_cursor.lastrowid
            for col in columns_by_table.get((schema, table_name), []):
                db_cursor.execute(
                    """INSERT INTO columns (table_id, column_name, data_type, ordinal_position)
                       VALUES (?, ?, ?, ?)""",
                    (table_id, col["name"], col["type"], col["position"]),
                )
            count += 1
            logger.debug("Synced Redshift table: %s.%s", schema, table_name)
        except Exception:
            logger.warning("Failed to process Redshift table: %s.%s", schema, table_name, exc_info=True)

    conn.commit()
    logger.info("Redshift sync complete: %d tables", count)

    # Sync schema mappings
    db_cursor.execute("DELETE FROM schema_mappings")
    mapping_count = 0
    for redshift_schema, glue_database, esoptions in rs_schema_mappings:
        try:
            # Parse region from esoptions if available (format: key=value, key=value)
            region = None
            catalog = None
            if esoptions:
                for opt in esoptions.split(","):
                    opt = opt.strip()
                    if opt.startswith("region="):
                        region = opt.split("=", 1)[1]
                    elif opt.startswith("catalog_id="):
                        catalog = opt.split("=", 1)[1]
            db_cursor.execute(
                """INSERT INTO schema_mappings (glue_database, redshift_schema, catalog_name, region)
                   VALUES (?, ?, ?, ?)""",
                (glue_database, redshift_schema, catalog, region),
            )
            mapping_count += 1
            logger.debug("Synced schema mapping: %s -> %s", glue_database, redshift_schema)
        except Exception:
            logger.warning("Failed to process schema mapping: %s -> %s", glue_database, redshift_schema, exc_info=True)

    conn.commit()
    logger.info("Schema mappings sync complete: %d mappings", mapping_count)


def main():
    parser = argparse.ArgumentParser(description="Sync AWS Glue and Redshift schemas to a local SQLite catalog.")
    parser.add_argument("-o", "--output", default="./catalog.db", help="SQLite output path (default: ./catalog.db)")
    parser.add_argument("--skip-glue", action="store_true", help="Skip Glue sync")
    parser.add_argument("--skip-redshift", action="store_true", help="Skip Redshift sync")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--glue-database", help="Glue database name (required unless --skip-glue)")
    parser.add_argument("--redshift-host", help="Redshift host (required unless --skip-redshift)")
    parser.add_argument("--redshift-cluster", help="Redshift cluster identifier (required unless --skip-redshift)")
    parser.add_argument("--redshift-database", help="Redshift database name (required unless --skip-redshift)")
    parser.add_argument("--redshift-user", help="Redshift db_user (required unless --skip-redshift)")
    parser.add_argument("--redshift-region", default="eu-west-1", help="AWS region (default: eu-west-1)")
    parser.add_argument("--redshift-login-url", help="SAML IdP login URL for browser-based auth (required unless --skip-redshift)")
    parser.add_argument("--saml-timeout", type=int, default=120,
                        help="Timeout in seconds for SAML authentication (default: 120)")
    args = parser.parse_args()

    # Validate required arguments based on skip flags
    if not args.skip_glue and not args.glue_database:
        parser.error("--glue-database is required unless --skip-glue is set")
    if not args.skip_redshift:
        missing = []
        if not args.redshift_host:
            missing.append("--redshift-host")
        if not args.redshift_cluster:
            missing.append("--redshift-cluster")
        if not args.redshift_database:
            missing.append("--redshift-database")
        if not args.redshift_user:
            missing.append("--redshift-user")
        if not args.redshift_login_url:
            missing.append("--redshift-login-url")
        if missing:
            parser.error(f"The following arguments are required unless --skip-redshift is set: {', '.join(missing)}")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    conn = init_db(args.output)
    try:
        if not args.skip_glue:
            sync_glue(conn, args.glue_database)
        if not args.skip_redshift:
            sync_redshift(conn, args.redshift_host, args.redshift_cluster,
                          args.redshift_database, args.redshift_user,
                          args.redshift_region, args.redshift_login_url,
                          args.saml_timeout)
    finally:
        conn.close()

    logger.info("Catalog written to %s", args.output)


if __name__ == "__main__":
    main()
