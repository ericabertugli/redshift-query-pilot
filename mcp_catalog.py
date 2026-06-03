#!/usr/bin/env python3
"""MCP server exposing the schema catalog — no Redshift dependency."""

import os
import signal
import sqlite3
from functools import wraps
from pathlib import Path

from mcp.server.fastmcp import FastMCP

DB_PATH = os.environ.get("CATALOG_DB_PATH", Path(__file__).parent / "catalog.db")
TOOL_TIMEOUT = int(os.environ.get("MCP_TOOL_TIMEOUT", 30))

mcp = FastMCP("mcp-catalog")


class ToolTimeoutError(Exception):
    pass


def with_timeout(seconds: int = TOOL_TIMEOUT):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def timeout_handler(signum, frame):
                raise ToolTimeoutError(f"Operation timed out after {seconds} seconds")
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


_SQL_OPS = {"=": "=", "LIKE": "LIKE"}


def _get_knowledge_table_descs(conn, table_name: str, exact: bool = True) -> list[dict]:
    try:
        op_key = "=" if exact else "LIKE"
        op = _SQL_OPS[op_key]
        val = table_name if exact else f"%{table_name}%"
        cursor = conn.execute(
            f"SELECT source_file, description FROM table_descriptions WHERE table_name {op} ? ORDER BY source_file",
            (val,),
        )
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.OperationalError:
        return []


def _get_knowledge_col_descs(conn, table_name: str, exact: bool = True) -> dict[str, list[dict]]:
    try:
        op_key = "=" if exact else "LIKE"
        op = _SQL_OPS[op_key]
        val = table_name if exact else f"%{table_name}%"
        cursor = conn.execute(
            f"SELECT column_name, source_file, description FROM column_descriptions WHERE table_name {op} ? ORDER BY column_name, source_file",
            (val,),
        )
        result: dict[str, list[dict]] = {}
        for row in cursor.fetchall():
            col = row["column_name"]
            if col not in result:
                result[col] = []
            result[col].append({"source_file": row["source_file"], "description": row["description"]})
        return result
    except sqlite3.OperationalError:
        return {}


@mcp.tool()
@with_timeout()
def get_field_descriptions(
    table_name: str, column_name: str | None = None
) -> str:
    """Get knowledge-base descriptions for a table and its columns.

    Searches across all knowledge YAML files and returns descriptions
    with source file attribution.

    Args:
        table_name: Table name to search for (partial match supported)
        column_name: Optional column name to filter (partial match supported)

    Returns:
        Descriptions from all matching knowledge files
    """
    with get_db() as conn:
        results = []

        table_descs = _get_knowledge_table_descs(conn, table_name, exact=False)
        if table_descs:
            results.append(f"=== Table descriptions for '*{table_name}*' ===")
            for row in table_descs:
                results.append(f"  [{row['source_file']}] {row['description']}")
            results.append("")

        try:
            col_query = """SELECT column_name, source_file, description FROM column_descriptions
                           WHERE table_name LIKE ?"""
            params: list[str] = [f"%{table_name}%"]
            if column_name:
                col_query += " AND column_name LIKE ?"
                params.append(f"%{column_name}%")
            col_query += " ORDER BY column_name, source_file"

            cursor = conn.execute(col_query, params)
            col_rows = cursor.fetchall()
        except sqlite3.OperationalError:
            col_rows = []

        if col_rows:
            results.append("=== Column descriptions ===")
            current_col = None
            for row in col_rows:
                if row["column_name"] != current_col:
                    current_col = row["column_name"]
                    results.append(f"\n  {current_col}:")
                results.append(f"    [{row['source_file']}] {row['description']}")

    if not results:
        return f"No knowledge descriptions found for '{table_name}'"

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
                results.append(
                    f"  {col['ordinal_position']:3}. {col['column_name']}: {col['data_type']}{partition_marker}{comment}"
                )

            results.append("")

        table_descs = _get_knowledge_table_descs(conn, table_name)
        col_descs = _get_knowledge_col_descs(conn, table_name)

        if table_descs or col_descs:
            results.append("--- Knowledge Base ---")
            if table_descs:
                results.append("Table descriptions:")
                for row in table_descs:
                    results.append(f"  [{row['source_file']}] {row['description']}")
                results.append("")
            if col_descs:
                results.append("Column descriptions:")
                for col_name, descs in col_descs.items():
                    for d in descs:
                        results.append(f"  {col_name}: [{d['source_file']}] {d['description']}")
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
    results.append(
        "TIP: Always include these columns in your WHERE clause to avoid full S3 scans!"
    )

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
        if row["region"]:
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
    mcp.run()


if __name__ == "__main__":
    main()
