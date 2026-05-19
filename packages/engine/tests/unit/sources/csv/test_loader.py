"""Unit tests for CSV loader."""

from dataraum.sources.csv import CSVLoader


class TestSanitizeTableName:
    """Tests for table name sanitization."""

    def test_strips_extension(self):
        loader = CSVLoader()
        assert loader._sanitize_table_name("My Table.csv") == "my_table"

    def test_replaces_dashes(self):
        loader = CSVLoader()
        assert loader._sanitize_table_name("table-name.csv") == "table_name"

    def test_prefixes_numeric_start(self):
        loader = CSVLoader()
        assert loader._sanitize_table_name("123table") == "t_123table"

    def test_preserves_valid_name(self):
        loader = CSVLoader()
        assert loader._sanitize_table_name("valid_name") == "valid_name"

    def test_replaces_spaces(self):
        loader = CSVLoader()
        assert loader._sanitize_table_name("my table") == "my_table"

    def test_lowercases(self):
        loader = CSVLoader()
        assert loader._sanitize_table_name("MyTable") == "mytable"
