from pathlib import Path
import unittest
from unittest.mock import patch
from snowflake_objects.snowflake_database_object import SnowflakeDatabaseObject


class TestSnowflakeDatabaseObject(unittest.TestCase):

    def test_initialization(self):
        db_object = SnowflakeDatabaseObject(database="test_db", schema="test_schema")
        self.assertEqual(db_object.database, "test_db")
        self.assertEqual(db_object.schema, "test_schema")

    def test_str_method(self):
        db_object = SnowflakeDatabaseObject(database="test_db", schema="test_schema")
        self.assertEqual(str(db_object), "test_db.test_schema")

    @patch("pathlib.Path.rglob")
    def test_find_folder_path_folder_found(self, mock_rglob):
        # Mock the rglob to simulate the directory structure
        mock_rglob.return_value = [
            Path("./tests/test_path/sql_templates_test"),
        ]

        db_object = SnowflakeDatabaseObject(database="test_db", schema="test_schema")
        result = db_object._find_folder_path("sql_templates_test", "./tests/")

        self.assertEqual(result, str(Path("./tests/test_path/sql_templates_test")))

    @patch("pathlib.Path.rglob")
    def test_find_sql_templates_folder_not_found(self, mock_rglob):
        # Mock the rglob to simulate the directory structure
        mock_rglob.return_value = []

        db_object = SnowflakeDatabaseObject(database="test_db", schema="test_schema")
        result = db_object._find_folder_path("sql_templates_test", "./tests/")
        self.assertIsNone(result)

    @patch("pathlib.Path.read_text")
    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.rglob")
    def test_get_sql_template_folder_found(
        self, mock_rglob, mock_is_dir, mock_read_text
    ):
        # Mock the rglob to simulate the directory structure
        mock_rglob.return_value = [
            Path("./tests/test_path/test_path_2/sql_templates_test"),
        ]
        mock_is_dir.return_value = True
        mock_read_text.return_value = (
            """create or alter table {table_name}\n{columns}\nCOMMENT = '{comment}';"""
        )

        db_object = SnowflakeDatabaseObject(database="test_db", schema="test_schema")
        result = db_object.get_sql_template("passwords_do_not_drop.sql", "./tests/")
        self.assertEqual(
            result,
            """create or alter table {table_name}\n{columns}\nCOMMENT = '{comment}';""",
        )


if __name__ == "__main__":
    unittest.main()
