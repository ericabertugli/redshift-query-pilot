import importlib

import pytest

import mcp_catalog


@pytest.fixture(autouse=True)
def _patch_db_path(monkeypatch, sample_catalog):
    monkeypatch.setattr(mcp_catalog, "DB_PATH", sample_catalog)


class TestSearchTables:
    def test_finds_by_keyword(self):
        result = mcp_catalog.search_tables(keyword="users")
        assert "[glue]" in result
        assert "analytics_db.users" in result

    def test_no_match(self):
        result = mcp_catalog.search_tables(keyword="nonexistent")
        assert "No tables found" in result

    def test_partial_match(self):
        result = mcp_catalog.search_tables(keyword="use")
        assert "users" in result

    def test_filter_by_source_glue(self):
        result = mcp_catalog.search_tables(keyword="%", source="glue")
        assert "[glue]" in result
        assert "[redshift]" not in result

    def test_filter_by_source_redshift(self):
        result = mcp_catalog.search_tables(keyword="%", source="redshift")
        assert "[redshift]" in result
        assert "[glue]" not in result

    def test_includes_storage_format(self):
        result = mcp_catalog.search_tables(keyword="events")
        assert "JSON" in result

    def test_includes_table_type(self):
        result = mcp_catalog.search_tables(keyword="users")
        assert "EXTERNAL_TABLE" in result

    def test_includes_location_if_present(self):
        result = mcp_catalog.search_tables(keyword="users")
        assert "analytics_db.users" in result


class TestGetTableSchema:
    def test_exact_match(self):
        result = mcp_catalog.get_table_schema(table_name="users")
        assert "[glue] analytics_db.users" in result
        assert "user_id" in result
        assert "int" in result
        assert "name" in result
        assert "string" in result
        assert "user_id" in result

    def test_table_not_found(self):
        result = mcp_catalog.get_table_schema(table_name="ghost")
        assert "not found" in result.lower()

    def test_with_database_name(self):
        result = mcp_catalog.get_table_schema(table_name="users", database_name="analytics_db")
        assert "[glue] analytics_db.users" in result

    def test_with_wrong_database_returns_not_found(self):
        result = mcp_catalog.get_table_schema(table_name="users", database_name="wrong_db")
        assert "not found" in result.lower()

    def test_shows_description(self):
        result = mcp_catalog.get_table_schema(table_name="users")
        assert "User profiles table" in result

    def test_shows_partition_key(self):
        result = mcp_catalog.get_table_schema(table_name="users")
        assert "PARTITION KEY" in result
        assert "dt" in result

    def test_shows_knowledge_base_section(self):
        result = mcp_catalog.get_table_schema(table_name="users")
        assert "Knowledge Base" in result
        assert "dbt-docs.yml" in result

    def test_includes_comment_on_column(self):
        result = mcp_catalog.get_table_schema(table_name="users")
        assert "unique user id" in result


class TestListPartitionKeys:
    def test_returns_partition_keys(self):
        result = mcp_catalog.list_partition_keys(table_name="users")
        assert "dt" in result
        assert "date" in result

    def test_table_no_partitions(self):
        result = mcp_catalog.list_partition_keys(table_name="orders")
        assert "no partition keys" in result.lower()

    def test_table_not_found(self):
        result = mcp_catalog.list_partition_keys(table_name="ghost")
        assert "not found" in result.lower()

    def test_with_database_name(self):
        result = mcp_catalog.list_partition_keys(table_name="users", database_name="analytics_db")
        assert "dt" in result

    def test_tips_included(self):
        result = mcp_catalog.list_partition_keys(table_name="users")
        assert "TIP" in result
        assert "WHERE" in result


class TestGetSchemaMapping:
    def test_all_mappings(self):
        result = mcp_catalog.get_schema_mapping()
        assert "analytics_db" in result
        assert "analytics" in result

    def test_specific_database(self):
        result = mcp_catalog.get_schema_mapping(glue_database="analytics_db")
        assert "analytics_db → analytics" in result

    def test_not_found(self):
        result = mcp_catalog.get_schema_mapping(glue_database="unknown_db")
        assert "No schema mapping found" in result

    def test_no_mappings_at_all(self, temp_db):
        conn = mcp_catalog.get_db()
        conn.execute("DELETE FROM schema_mappings")
        conn.commit()

        result = mcp_catalog.get_schema_mapping()
        assert "No schema mappings found" in result

    def test_includes_region(self):
        result = mcp_catalog.get_schema_mapping()
        assert "eu-west-1" in result


class TestFindColumns:
    def test_finds_column_in_tables(self):
        result = mcp_catalog.find_columns(column_name="user_id")
        assert "users.user_id" in result
        assert "orders.user_id" in result
        assert "int" in result

    def test_no_match(self):
        result = mcp_catalog.find_columns(column_name="zzzz")
        assert "No columns found" in result

    def test_partial_match(self):
        result = mcp_catalog.find_columns(column_name="event")
        assert "event_id" in result
        assert "event_type" in result

    def test_filter_by_source(self):
        result = mcp_catalog.find_columns(column_name="%", source="redshift")
        assert "[redshift]" in result
        assert "[glue]" not in result

    def test_marks_partition_keys(self):
        result = mcp_catalog.find_columns(column_name="dt")
        assert "[PK]" in result


class TestGetFieldDescriptions:
    def test_finds_table_descriptions(self):
        result = mcp_catalog.get_field_descriptions(table_name="users")
        assert "User profiles with PII data" in result
        assert "dbt-docs.yml" in result

    def test_finds_column_descriptions(self):
        result = mcp_catalog.get_field_descriptions(table_name="users")
        assert "User email address" in result

    def test_filter_by_column_name(self):
        result = mcp_catalog.get_field_descriptions(table_name="users", column_name="email")
        assert "User email address" in result

    def test_no_match(self):
        result = mcp_catalog.get_field_descriptions(table_name="ghost")
        assert "No knowledge descriptions found" in result

    def test_partial_table_match(self):
        result = mcp_catalog.get_field_descriptions(table_name="use")
        assert "users" in result or "User" in result
