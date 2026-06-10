import time
from unittest.mock import MagicMock

import pytest

import mcp_query


class TestValidateSql:
    def test_allows_select(self):
        assert mcp_query._validate_sql("SELECT * FROM users") is None

    def test_allows_with_cte(self):
        assert mcp_query._validate_sql("WITH cte AS (SELECT 1) SELECT * FROM cte") is None

    def test_allows_show(self):
        assert mcp_query._validate_sql("SHOW SCHEMAS") is None

    def test_allows_explain(self):
        assert mcp_query._validate_sql("EXPLAIN SELECT * FROM users") is None

    def test_allows_create_temp_table(self):
        assert mcp_query._validate_sql("CREATE TEMP TABLE tmp AS SELECT * FROM users") is None
        assert mcp_query._validate_sql("CREATE TEMPORARY TABLE tmp AS SELECT 1") is None

    def test_rejects_insert(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            mcp_query._validate_sql("INSERT INTO users VALUES (1)")

    def test_rejects_update(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            mcp_query._validate_sql("UPDATE users SET name = 'x'")

    def test_rejects_delete(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            mcp_query._validate_sql("DELETE FROM users")

    def test_rejects_drop(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            mcp_query._validate_sql("DROP TABLE users")

    def test_rejects_alter(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            mcp_query._validate_sql("ALTER TABLE users ADD COLUMN x INT")

    def test_rejects_create_table_permanent(self):
        with pytest.raises(ValueError, match="Only SELECT"):
            mcp_query._validate_sql("CREATE TABLE users (id INT)")

    def test_empty_string_raises_error(self):
        with pytest.raises(ValueError):
            mcp_query._validate_sql("")


class TestFormatResults:
    def test_empty_rows(self):
        result = mcp_query._format_results(["id", "name"], [], False)
        assert "0 rows" in result

    def test_single_row(self):
        result = mcp_query._format_results(["id", "name"], [(1, "alice")], False)
        assert "1 row" in result
        assert "alice" in result

    def test_multiple_rows(self):
        rows = [(1, "alice"), (2, "bob")]
        result = mcp_query._format_results(["id", "name"], rows, False)
        assert "2 rows" in result
        assert "alice" in result
        assert "bob" in result

    def test_truncated_flag(self):
        result = mcp_query._format_results(["id"], [(1,), (2,)], True)
        assert "TRUNCATED" in result

    def test_null_values(self):
        result = mcp_query._format_results(["id", "name"], [(1, None)], False)
        assert "NULL" in result

    def test_truncates_long_values(self):
        long_val = "x" * 100
        result = mcp_query._format_results(["col"], [(long_val,)], False)
        assert "…" in result

    def test_column_widths_grow_with_content(self):
        result = mcp_query._format_results(["a", "b"], [("short", "very_long_value_here")], False)
        assert "very_long_value_here" in result


class TestValidateWithExplain:
    def test_skips_explain_statements(self, monkeypatch):
        spy = MagicMock()
        monkeypatch.setattr(mcp_query, "_get_redshift_connection", spy)
        mcp_query._validate_with_explain(None, "EXPLAIN SELECT 1")
        spy.assert_not_called()

    def test_skips_show_statements(self, monkeypatch):
        spy = MagicMock()
        monkeypatch.setattr(mcp_query, "_get_redshift_connection", spy)
        mcp_query._validate_with_explain(None, "SHOW TABLES")
        spy.assert_not_called()

    def test_skips_create_temp_table_without_as(self, monkeypatch):
        spy = MagicMock()
        monkeypatch.setattr(mcp_query, "_get_redshift_connection", spy)
        mcp_query._validate_with_explain(None, "CREATE TEMP TABLE tmp (id INT)")
        spy.assert_not_called()

    def test_runs_explain_for_select(self):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mcp_query._validate_with_explain(mock_conn, "SELECT 1")
        mock_cursor.execute.assert_called_once()

    def test_raises_on_explain_failure(self):
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("syntax error")
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with pytest.raises(ValueError, match="SQL validation failed"):
            mcp_query._validate_with_explain(mock_conn, "SELECT invalid SQL")


class TestCreateRedshiftConnection:
    def test_missing_env_vars_raises_value_error(self, monkeypatch):
        monkeypatch.setattr(mcp_query, "RS_HOST", "")
        monkeypatch.setattr(mcp_query, "RS_DATABASE", "")
        monkeypatch.setattr(mcp_query, "RS_USER", "")
        monkeypatch.setattr(mcp_query, "RS_LOGIN_URL", "")
        monkeypatch.setattr(mcp_query, "RS_PASSWORD", "")

        with pytest.raises(ValueError, match="Missing required env vars"):
            mcp_query._create_redshift_connection()

    def test_password_auth_with_minimal_env(self, monkeypatch):
        monkeypatch.setattr(mcp_query, "RS_HOST", "h")
        monkeypatch.setattr(mcp_query, "RS_DATABASE", "d")
        monkeypatch.setattr(mcp_query, "RS_USER", "u")
        monkeypatch.setattr(mcp_query, "RS_PASSWORD", "p")
        monkeypatch.setattr(mcp_query, "RS_LOGIN_URL", "")

        mock_conn = object()
        monkeypatch.setattr(mcp_query, "connect_password", lambda host, database, user, password: mock_conn)

        result = mcp_query._create_redshift_connection()
        assert result is mock_conn


