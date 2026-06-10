#!/usr/bin/env python3
"""MCP server for executing SQL queries against Redshift."""

import os
import re
import signal
from functools import wraps

from mcp.server.fastmcp import FastMCP

from redshift_conn import connect_password, connect_saml

RS_HOST = os.environ.get("REDSHIFT_HOST", "")
RS_CLUSTER = os.environ.get("REDSHIFT_CLUSTER", "")
RS_DATABASE = os.environ.get("REDSHIFT_DATABASE", "")
RS_USER = os.environ.get("REDSHIFT_USER", "")
RS_REGION = os.environ.get("REDSHIFT_REGION", "eu-west-1")
RS_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "")
RS_LOGIN_URL = os.environ.get("REDSHIFT_LOGIN_URL", "")
RS_QUERY_TIMEOUT = int(os.environ.get("REDSHIFT_QUERY_TIMEOUT", 60))
RS_MAX_ROWS = int(os.environ.get("REDSHIFT_MAX_ROWS", 1000))

_ALLOWED_SQL_RE = re.compile(
    r"^\s*(SELECT|WITH|SHOW|EXPLAIN|CREATE\s+TEMP(ORARY)?\s+TABLE)\b",
    re.IGNORECASE,
)

mcp = FastMCP("mcp_query")

_redshift_conn = None


class ToolTimeoutError(Exception):
    pass


def with_timeout(seconds: int):
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


def _create_redshift_connection():
    use_saml = bool(RS_LOGIN_URL)

    common_required = [
        ("REDSHIFT_HOST", RS_HOST),
        ("REDSHIFT_DATABASE", RS_DATABASE),
        ("REDSHIFT_USER", RS_USER),
    ]
    saml_required = [("REDSHIFT_CLUSTER", RS_CLUSTER)]
    password_required = [("REDSHIFT_PASSWORD", RS_PASSWORD)]

    required = common_required + (saml_required if use_saml else password_required)
    missing = [name for name, val in required if not val]
    if missing:
        raise ValueError(
            f"Missing required env vars for Redshift connection: {', '.join(missing)}"
        )

    if use_saml:
        return connect_saml(
            host=RS_HOST,
            cluster=RS_CLUSTER,
            database=RS_DATABASE,
            user=RS_USER,
            login_url=RS_LOGIN_URL,
            region=RS_REGION,
        )

    return connect_password(
        host=RS_HOST,
        database=RS_DATABASE,
        user=RS_USER,
        password=RS_PASSWORD,
    )


def _get_redshift_connection():
    global _redshift_conn

    if _redshift_conn is not None:
        try:
            cursor = _redshift_conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return _redshift_conn
        except Exception:
            try:
                _redshift_conn.close()
            except Exception:
                pass
            _redshift_conn = None

    _redshift_conn = _create_redshift_connection()
    return _redshift_conn


def _close_redshift_connection():
    global _redshift_conn
    if _redshift_conn is not None:
        try:
            _redshift_conn.close()
        except Exception:
            pass
        _redshift_conn = None


def _validate_sql(sql: str) -> None:
    if not _ALLOWED_SQL_RE.match(sql):
        raise ValueError(
            "Only SELECT, WITH, SHOW, EXPLAIN, and CREATE TEMP TABLE statements are allowed. "
            "DML (INSERT/UPDATE/DELETE) and DDL on permanent objects are blocked."
        )


def _validate_with_explain(conn, sql: str) -> None:
    upper = sql.strip().upper()

    if upper.startswith("EXPLAIN") or upper.startswith("SHOW"):
        return

    if re.match(r"^\s*CREATE\s+TEMP(?:ORARY)?\s+TABLE\b", sql, re.IGNORECASE):
        if not re.search(r"\bAS\s", sql, re.IGNORECASE):
            return

    cursor = conn.cursor()
    try:
        cursor.execute(f"EXPLAIN {sql}")
    except Exception as e:
        raise ValueError(f"SQL validation failed: {e}")
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def _format_results(columns: list[str], rows: list[tuple], truncated: bool) -> str:
    if not rows:
        return "Query returned 0 rows."

    col_widths = [len(c) for c in columns]
    str_rows = []
    for row in rows:
        str_row = [str(v) if v is not None else "NULL" for v in row]
        for i, val in enumerate(str_row):
            col_widths[i] = max(col_widths[i], min(len(val), 80))
        str_rows.append(str_row)

    def truncate_val(val, width):
        return val[:width] + "…" if len(val) > width else val.ljust(width)

    header = " | ".join(c.ljust(col_widths[i]) for i, c in enumerate(columns))
    separator = "-+-".join("-" * w for w in col_widths)
    lines = [header, separator]
    for str_row in str_rows:
        lines.append(
            " | ".join(truncate_val(v, col_widths[i]) for i, v in enumerate(str_row))
        )

    footer = f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})"
    if truncated:
        footer += f" [TRUNCATED at {RS_MAX_ROWS} rows — add LIMIT or narrow filters]"
    lines.append(footer)
    return "\n".join(lines)


@mcp.tool()
@with_timeout(RS_QUERY_TIMEOUT)
def run_query(sql: str, max_rows: int | None = None) -> str:
    """Execute a SQL query against Redshift and return the results.

    Only SELECT, WITH (CTE), SHOW, EXPLAIN, and CREATE TEMP TABLE statements
    are allowed. DML on permanent tables is blocked.

    SQL is automatically validated with EXPLAIN before execution to catch
    errors early and avoid wasting time on invalid queries.

    The connection is cached and reused across calls to avoid repeated
    browser authentication.

    Args:
        sql: The SQL query to execute
        max_rows: Maximum rows to return (default from REDSHIFT_MAX_ROWS env var, max 5000)

    Returns:
        Formatted query results as a text table, or a status message for DDL
    """
    _validate_sql(sql)
    if max_rows is not None and max_rows <= 0:
        raise ValueError("max_rows must be a positive integer")
    row_limit = min(max_rows or RS_MAX_ROWS, 5000)

    conn = _get_redshift_connection()
    _validate_with_explain(conn, sql)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)

        if cursor.description is None:
            conn.commit()
            return "Statement executed successfully."

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchmany(row_limit + 1)
        truncated = len(rows) > row_limit
        if truncated:
            rows = rows[:row_limit]

        conn.commit()
        return _format_results(columns, rows, truncated)
    except Exception as e:
        _close_redshift_connection()
        return f"Query error: {e}"
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def main():
    mcp.run()


if __name__ == "__main__":
    main()
