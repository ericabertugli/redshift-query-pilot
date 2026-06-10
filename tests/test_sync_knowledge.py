import sqlite3
from pathlib import Path

import yaml

from sync_knowledge import _now_iso, _parse_yaml_file, sync_knowledge


class TestNowIso:
    def test_returns_string(self):
        result = _now_iso()
        assert isinstance(result, str)
        assert "T" in result


class TestParseYamlFile:
    def test_valid_file_with_tables_and_columns(self, tmp_path):
        content = {
            "tables": [
                {
                    "name": "users",
                    "database": "analytics_db",
                    "description": "User profile data",
                    "columns": [
                        {"name": "user_id", "description": "Unique identifier"},
                        {"name": "email", "description": "Email address"},
                    ],
                }
            ]
        }
        p = tmp_path / "knowledge.yml"
        p.write_text(yaml.dump(content))

        table_rows, col_rows = _parse_yaml_file(p, "knowledge.yml")

        assert len(table_rows) == 1
        assert len(col_rows) == 2
        assert table_rows[0] == ("analytics_db", "users", "knowledge.yml", "User profile data")
        assert col_rows[0] == ("analytics_db", "users", "user_id", "knowledge.yml", "Unique identifier")

    def test_skips_table_without_description(self, tmp_path):
        content = {"tables": [{"name": "users", "database": "analytics_db"}]}
        p = tmp_path / "no_desc.yml"
        p.write_text(yaml.dump(content))

        table_rows, col_rows = _parse_yaml_file(p, "no_desc.yml")
        assert len(table_rows) == 0
        assert len(col_rows) == 0

    def test_skips_table_without_name(self, tmp_path):
        content = {"tables": [{"database": "analytics_db", "description": "desc"}]}
        p = tmp_path / "no_name.yml"
        p.write_text(yaml.dump(content))

        table_rows, col_rows = _parse_yaml_file(p, "no_name.yml")
        assert len(table_rows) == 0

    def test_skips_non_dict_table_entries(self, tmp_path):
        content = {"tables": ["not_a_dict"]}
        p = tmp_path / "bad.yml"
        p.write_text(yaml.dump(content))

        table_rows, col_rows = _parse_yaml_file(p, "bad.yml")
        assert len(table_rows) == 0

    def test_invalid_yaml_returns_empty(self, tmp_path):
        p = tmp_path / "invalid.yml"
        p.write_text("{invalid: yaml: [")

        table_rows, col_rows = _parse_yaml_file(p, "invalid.yml")
        assert len(table_rows) == 0
        assert len(col_rows) == 0

    def test_no_tables_key_returns_empty(self, tmp_path):
        p = tmp_path / "no_tables.yml"
        p.write_text(yaml.dump({"other_key": "value"}))

        table_rows, col_rows = _parse_yaml_file(p, "no_tables.yml")
        assert len(table_rows) == 0

    def test_skips_non_dict_columns(self, tmp_path):
        content = {
            "tables": [{"name": "users", "description": "desc", "columns": ["not_a_dict"]}]
        }
        p = tmp_path / "bad_col.yml"
        p.write_text(yaml.dump(content))

        _, col_rows = _parse_yaml_file(p, "bad_col.yml")
        assert len(col_rows) == 0

    def test_skips_column_without_name(self, tmp_path):
        content = {
            "tables": [{"name": "users", "description": "desc", "columns": [{"description": "just a desc"}]}]
        }
        p = tmp_path / "col_no_name.yml"
        p.write_text(yaml.dump(content))

        _, col_rows = _parse_yaml_file(p, "col_no_name.yml")
        assert len(col_rows) == 0

    def test_skips_column_without_description(self, tmp_path):
        content = {
            "tables": [{"name": "users", "description": "desc", "columns": [{"name": "user_id"}]}]
        }
        p = tmp_path / "col_no_desc.yml"
        p.write_text(yaml.dump(content))

        _, col_rows = _parse_yaml_file(p, "col_no_desc.yml")
        assert len(col_rows) == 0

    def test_trims_whitespace_from_descriptions(self, tmp_path):
        content = {
            "tables": [
                {
                    "name": "users",
                    "description": "  padded desc  ",
                    "columns": [{"name": "user_id", "description": "  padded  "}],
                }
            ]
        }
        p = tmp_path / "padded.yml"
        p.write_text(yaml.dump(content))

        table_rows, col_rows = _parse_yaml_file(p, "padded.yml")
        assert table_rows[0][3] == "padded desc"
        assert col_rows[0][4] == "padded"


class TestSyncKnowledge:
    def test_syncs_yaml_files_to_db(self, temp_db, tmp_path, monkeypatch):
        content = {
            "tables": [
                {
                    "name": "users",
                    "database": "analytics",
                    "description": "User table",
                    "columns": [{"name": "id", "description": "Primary key"}],
                }
            ]
        }
        (tmp_path / "docs.yml").write_text(yaml.dump(content))
        monkeypatch.setattr("sync_knowledge.KNOWLEDGE_DIR", tmp_path)

        t, c = sync_knowledge(db_path=temp_db)
        assert t == 1
        assert c == 1

        conn = sqlite3.connect(temp_db)
        td = conn.execute("SELECT * FROM table_descriptions").fetchone()
        assert td[2] == "users"
        assert td[4] == "User table"
        cd = conn.execute("SELECT * FROM column_descriptions").fetchone()
        assert cd[3] == "id"
        assert cd[5] == "Primary key"
        conn.close()

    def test_multiple_yaml_files(self, temp_db, tmp_path, monkeypatch):
        y1 = {"tables": [{"name": "t1", "description": "First table"}]}
        y2 = {"tables": [{"name": "t2", "description": "Second table"}]}
        (tmp_path / "a.yml").write_text(yaml.dump(y1))
        (tmp_path / "b.yml").write_text(yaml.dump(y2))
        monkeypatch.setattr("sync_knowledge.KNOWLEDGE_DIR", tmp_path)

        t, c = sync_knowledge(db_path=temp_db)
        assert t == 2

    def test_upserts_existing_entry(self, temp_db, tmp_path, monkeypatch):
        content = {"tables": [{"name": "users", "description": "Original"}]}
        (tmp_path / "doc.yml").write_text(yaml.dump(content))
        monkeypatch.setattr("sync_knowledge.KNOWLEDGE_DIR", tmp_path)
        sync_knowledge(db_path=temp_db)

        content["tables"][0]["description"] = "Updated"
        (tmp_path / "doc.yml").write_text(yaml.dump(content))
        sync_knowledge(db_path=temp_db)

        conn = sqlite3.connect(temp_db)
        desc = conn.execute(
            "SELECT description FROM table_descriptions WHERE table_name='users'"
        ).fetchone()[0]
        assert desc == "Updated"
        conn.close()

    def test_returns_zeroes_when_no_knowledge_dir(self, temp_db, monkeypatch):
        monkeypatch.setattr("sync_knowledge.KNOWLEDGE_DIR", Path("/nonexistent"))
        t, c = sync_knowledge(db_path=temp_db)
        assert t == 0
        assert c == 0

    def test_skips_non_yml_files(self, temp_db, tmp_path, monkeypatch):
        (tmp_path / "notes.md").write_text("# not yaml")
        (tmp_path / "data.txt").write_text("not yaml")
        monkeypatch.setattr("sync_knowledge.KNOWLEDGE_DIR", tmp_path)

        t, c = sync_knowledge(db_path=temp_db)
        assert t == 0
        assert c == 0
