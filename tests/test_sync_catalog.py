import sqlite3

from sync_catalog import detect_storage_format, init_db


class TestDetectStorageFormat:
    def test_parquet_by_input_format(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "SerdeInfo": {},
            }
        }
        assert detect_storage_format(table) == "Parquet"

    def test_parquet_by_serde(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            }
        }
        assert detect_storage_format(table) == "Parquet"

    def test_orc_by_input_format(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "SerdeInfo": {},
            }
        }
        assert detect_storage_format(table) == "ORC"

    def test_orc_by_serde(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.orc.OrcSerde"},
            }
        }
        assert detect_storage_format(table) == "ORC"

    def test_avro(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.avro.AvroSerDe"},
            }
        }
        assert detect_storage_format(table) == "Avro"

    def test_json(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "",
                "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"},
            }
        }
        assert detect_storage_format(table) == "JSON"

    def test_csv_with_lazy_serde(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
            }
        }
        assert detect_storage_format(table) == "CSV"

    def test_csv_with_opencsv_serde(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde"},
            }
        }
        assert detect_storage_format(table) == "CSV"

    def test_text_plain(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": {"SerializationLibrary": "custom.serde.CustomSerDe"},
            }
        }
        assert detect_storage_format(table) == "Text"

    def test_unknown_input_format(self):
        table = {"StorageDescriptor": {"InputFormat": "com.example.CustomInputFormat", "SerdeInfo": {}}}
        assert detect_storage_format(table) == "CustomInputFormat"

    def test_no_storage_descriptor(self):
        assert detect_storage_format({}) is None

    def test_no_input_format_no_serde(self):
        table = {"StorageDescriptor": {"InputFormat": "", "SerdeInfo": {}}}
        assert detect_storage_format(table) is None

    def test_csv_with_text_input_and_csv_serde(self):
        table = {
            "StorageDescriptor": {
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "SerdeInfo": {"SerializationLibrary": "csv.serde.CSVSerde"},
            }
        }
        assert detect_storage_format(table) == "CSV"


class TestInitDb:
    def test_creates_required_tables(self, temp_db):
        conn = init_db(temp_db)
        try:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            assert "tables" in tables
            assert "columns" in tables
            assert "schema_mappings" in tables
        finally:
            conn.close()

    def test_returns_connection_with_row_factory(self, temp_db):
        conn = init_db(temp_db)
        try:
            assert isinstance(conn, sqlite3.Connection)
        finally:
            conn.close()

    def test_idempotent(self, temp_db):
        conn1 = init_db(temp_db)
        conn1.close()
        conn2 = init_db(temp_db)
        conn2.close()

    def test_foreign_keys_enabled(self, temp_db):
        conn = init_db(temp_db)
        try:
            cursor = conn.execute("PRAGMA foreign_keys")
            assert cursor.fetchone()[0] == 1
        finally:
            conn.close()
