#!/usr/bin/env python3
"""MCP server exposing the schema catalog for SQL query assistance."""

import os
import sqlite3
import signal
from functools import wraps
from pathlib import Path

from mcp.server.fastmcp import FastMCP

DB_PATH = os.environ.get("CATALOG_DB_PATH", Path(__file__).parent / "catalog.db")
# Timeout in seconds for tool operations (default: 30 seconds)
TOOL_TIMEOUT = int(os.environ.get("MCP_TOOL_TIMEOUT", 30))

mcp = FastMCP("schema-catalog")


class ToolTimeoutError(Exception):
    """Raised when a tool operation times out."""
    pass


def with_timeout(seconds: int = TOOL_TIMEOUT):
    """Decorator that adds a timeout to a function using signals (Unix only)."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def timeout_handler(signum, frame):
                raise ToolTimeoutError(f"Operation timed out after {seconds} seconds")

            # Set up the timeout
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
            return result
        return wrapper
    return decorator


def get_db():
    """Get a database connection with timeout."""
    # SQLite timeout for lock acquisition (in seconds)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


@mcp.tool()
@with_timeout()
def search_tables(keyword: str, source: str | None = None) -> str:
    """Search for tables by name or keyword.

    Args:
        keyword: Search term to match against table names (case-insensitive)
        source: Optional filter by source ('glue' for Spectrum, 'redshift' for internal tables)

    Returns:
        List of matching tables with their source, database, and storage format
    """
    with get_db() as conn:
        query = """
            SELECT source, database_name, table_name, table_type, storage_format, location
            FROM tables
            WHERE table_name LIKE ?
        """
        params = [f"%{keyword}%"]

        if source:
            query += " AND source = ?"
            params.append(source)

        query += " ORDER BY source, database_name, table_name LIMIT 50"

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

    if not rows:
        return f"No tables found matching '{keyword}'"

    results = []
    for row in rows:
        line = f"[{row['source']}] {row['database_name']}.{row['table_name']}"
        if row["table_type"]:
            line += f" ({row['table_type']})"
        if row["storage_format"]:
            line += f" - {row['storage_format']}"
        results.append(line)

    return "\n".join(results)


@mcp.tool()
@with_timeout()
def get_table_schema(table_name: str, database_name: str | None = None) -> str:
    """Get the full schema (columns) for a specific table.

    Args:
        table_name: Name of the table
        database_name: Optional database/schema name to disambiguate

    Returns:
        Table metadata and list of columns with types and attributes
    """
    with get_db() as conn:
        # Find the table
        query = "SELECT * FROM tables WHERE table_name = ?"
        params = [table_name]
        if database_name:
            query += " AND database_name = ?"
            params.append(database_name)

        cursor = conn.execute(query, params)
        tables = cursor.fetchall()

        if not tables:
            return f"Table '{table_name}' not found"

        results = []
        for table in tables:
            header = f"=== [{table['source']}] {table['database_name']}.{table['table_name']} ==="
            results.append(header)

            if table["description"]:
                results.append(f"Description: {table['description']}")
            if table["table_type"]:
                results.append(f"Type: {table['table_type']}")
            if table["storage_format"]:
                results.append(f"Format: {table['storage_format']}")
            if table["location"]:
                results.append(f"Location: {table['location']}")

            results.append("\nColumns:")

            cursor = conn.execute(
                """SELECT column_name, data_type, ordinal_position, is_partition_key, comment
                   FROM columns WHERE table_id = ? ORDER BY ordinal_position""",
                (table["id"],),
            )
            columns = cursor.fetchall()

            for col in columns:
                partition_marker = " [PARTITION KEY]" if col["is_partition_key"] else ""
                comment = f" -- {col['comment']}" if col["comment"] else ""
                results.append(f"  {col['ordinal_position']:3}. {col['column_name']}: {col['data_type']}{partition_marker}{comment}")

            results.append("")

    return "\n".join(results)


@mcp.tool()
@with_timeout()
def list_partition_keys(table_name: str, database_name: str | None = None) -> str:
    """List partition keys for a table. Essential for optimizing Spectrum queries.

    Args:
        table_name: Name of the table
        database_name: Optional database/schema name to disambiguate

    Returns:
        List of partition key columns with their data types
    """
    with get_db() as conn:
        query = """
            SELECT t.source, t.database_name, t.table_name, c.column_name, c.data_type
            FROM tables t
            JOIN columns c ON t.id = c.table_id
            WHERE t.table_name = ? AND c.is_partition_key = 1
        """
        params = [table_name]
        if database_name:
            query += " AND t.database_name = ?"
            params.append(database_name)

        query += " ORDER BY c.ordinal_position"

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            # Check if table exists but has no partition keys
            exist_query = "SELECT 1 FROM tables WHERE table_name = ?"
            exist_params = [table_name]
            if database_name:
                exist_query += " AND database_name = ?"
                exist_params.append(database_name)
            cursor = conn.execute(exist_query, exist_params)
            exists = cursor.fetchone()

            if exists:
                return f"Table '{table_name}' has no partition keys (likely a Redshift internal table or unpartitioned Spectrum table)"
            return f"Table '{table_name}' not found"

    results = [f"Partition keys for {table_name}:", ""]
    for row in rows:
        results.append(f"  - {row['column_name']}: {row['data_type']}")

    results.append("")
    results.append("TIP: Always include these columns in your WHERE clause to avoid full S3 scans!")

    return "\n".join(results)


@mcp.tool()
@with_timeout()
def get_schema_mapping(glue_database: str | None = None) -> str:
    """Get the mapping between Glue databases and Redshift external schemas.

    External schemas in Redshift are linked to Glue databases. Use this to find
    the correct Redshift schema name to use in queries.

    Args:
        glue_database: Optional Glue database name to look up. If not provided, returns all mappings.

    Returns:
        Mapping of Glue databases to Redshift schema names
    """
    with get_db() as conn:
        if glue_database:
            cursor = conn.execute(
                """SELECT glue_database, redshift_schema, catalog_name, region
                   FROM schema_mappings WHERE glue_database = ?""",
                (glue_database,),
            )
        else:
            cursor = conn.execute(
                """SELECT glue_database, redshift_schema, catalog_name, region
                   FROM schema_mappings ORDER BY glue_database"""
            )

        rows = cursor.fetchall()

    if not rows:
        if glue_database:
            return f"No schema mapping found for Glue database '{glue_database}'"
        return "No schema mappings found. Run sync_catalog.py with Redshift sync enabled."

    results = ["Glue Database → Redshift Schema", "=" * 40]
    for row in rows:
        line = f"{row['glue_database']} → {row['redshift_schema']}"
        if row['region']:
            line += f" (region: {row['region']})"
        results.append(line)

    results.append("")
    results.append("Use the Redshift schema name in your SQL queries:")
    results.append(f"  SELECT * FROM {rows[0]['redshift_schema']}.table_name WHERE ...")

    return "\n".join(results)


@mcp.tool()
@with_timeout()
def find_columns(column_name: str, source: str | None = None) -> str:
    """Find tables containing a specific column name.

    Args:
        column_name: Column name to search for (case-insensitive)
        source: Optional filter by source ('glue' or 'redshift')

    Returns:
        List of tables containing the column
    """
    with get_db() as conn:
        query = """
            SELECT t.source, t.database_name, t.table_name, c.column_name, c.data_type, c.is_partition_key
            FROM columns c
            JOIN tables t ON c.table_id = t.id
            WHERE c.column_name LIKE ?
        """
        params = [f"%{column_name}%"]

        if source:
            query += " AND t.source = ?"
            params.append(source)

        query += " ORDER BY t.source, t.table_name, c.column_name LIMIT 100"

        cursor = conn.execute(query, params)
        rows = cursor.fetchall()

    if not rows:
        return f"No columns found matching '{column_name}'"

    results = []
    for row in rows:
        partition = " [PK]" if row["is_partition_key"] else ""
        results.append(
            f"[{row['source']}] {row['database_name']}.{row['table_name']}.{row['column_name']}: {row['data_type']}{partition}"
        )

    return "\n".join(results)


def main():
    """Entry point for the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
