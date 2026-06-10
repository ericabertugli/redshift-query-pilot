import os
import sqlite3
import tempfile

import pytest

from sync_catalog import SCHEMA_SQL
from sync_knowledge import KNOWLEDGE_SCHEMA_SQL


@pytest.fixture
def temp_db():
    db_fd, db_path = tempfile.mkstemp(suffix=".db")
    os.close(db_fd)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL + KNOWLEDGE_SCHEMA_SQL)
    conn.commit()
    conn.close()

    yield db_path

    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture
def sample_catalog(temp_db):
    conn = sqlite3.connect(temp_db)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        INSERT INTO tables (source, database_name, table_name, table_type, storage_format, description)
        VALUES ('glue', 'analytics_db', 'users', 'EXTERNAL_TABLE', 'Parquet', 'User profiles table');

        INSERT INTO tables (source, database_name, table_name, table_type)
        VALUES ('redshift', 'public', 'orders', 'BASE TABLE');

        INSERT INTO tables (source, database_name, table_name, table_type, storage_format)
        VALUES ('glue', 'analytics_db', 'events', 'EXTERNAL_TABLE', 'JSON');

        INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key, comment)
        VALUES (1, 'user_id', 'int', 1, 0, 'unique user id');

        INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key)
        VALUES (1, 'name', 'string', 2, 0);

        INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key)
        VALUES (1, 'email', 'string', 3, 0);

        INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key)
        VALUES (1, 'dt', 'date', 4, 1);

        INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key)
        VALUES (2, 'order_id', 'int', 1, 0);

        INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key)
        VALUES (2, 'user_id', 'int', 2, 0);

        INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key)
        VALUES (3, 'event_id', 'string', 1, 0);

        INSERT INTO columns (table_id, column_name, data_type, ordinal_position, is_partition_key)
        VALUES (3, 'event_type', 'string', 2, 0);

        INSERT INTO schema_mappings (glue_database, redshift_schema, region)
        VALUES ('analytics_db', 'analytics', 'eu-west-1');

        INSERT INTO table_descriptions (table_name, source_file, description)
        VALUES ('users', 'dbt-docs.yml', 'User profiles with PII data');

        INSERT INTO column_descriptions (table_name, column_name, source_file, description)
        VALUES ('users', 'email', 'dbt-docs.yml', 'User email address (PII)');
    """)
    conn.commit()
    conn.close()
    return temp_db


@pytest.fixture
def temp_knowledge_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir
