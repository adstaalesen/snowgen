import unittest
from unittest.mock import MagicMock, patch
from snowgen.main import create_new_schema_in, init
from snowgen.database_objects.snowflake_database_object import SnowflakeDatabaseObject
from snowgen.database_repository.database_repository import DatabaseRepository


class TestMain(unittest.TestCase):

    @patch("snowgen.main.inquirer.prompt")
    @patch.object(DatabaseRepository, "get_schema_template")
    @patch.object(DatabaseRepository, "get_snowflake_objects_path")
    @patch.object(DatabaseRepository, "get_sql_template")
    @patch.object(SnowflakeDatabaseObject, "save_object")
    def test_create_new_schema_in(
        self,
        mock_save_object,
        mock_get_sql_template,
        mock_get_snowflake_objects_path,
        mock_get_schema_template,
        mock_inquirer_prompt,
    ):
        mock_get_schema_template.return_value = {
            "database": "test_db",
            "role": "test_role",
            "file_formats": [
                {
                    "object_name": "test_file_format",
                    "template_name": "file_format_template",
                }
            ],
            "stages": [
                {"object_name": "test_stage", "template_name": "stage_template"}
            ],
            "sequences": [
                {"object_name": "test_sequence", "template_name": "sequence_template"}
            ],
            "tables": [
                {
                    "object_name": "test_table",
                    "template_name": "table_template",
                    "generate_columns_from_template": False,
                }
            ],
            "dynamic_tables": [
                {
                    "object_name": "test_dynamic_table",
                    "template_name": "dynamic_table_template",
                    "generate_columns_from_table": False,
                }
            ],
            "procedures": [
                {"object_name": "test_procedure", "template_name": "procedure_template"}
            ],
        }
        mock_inquirer_prompt.return_value = {"field_delimiter": ","}
        database_repository = DatabaseRepository()

        create_new_schema_in(database_repository, "test_schema", "test_template")

        self.assertEqual(mock_save_object.call_count, 6)

    @patch.object(DatabaseRepository, "setup")
    def test_init(self, mock_setup):
        database_repository = DatabaseRepository()
        init(database_repository)
        mock_setup.assert_called_once()


if __name__ == "__main__":
    unittest.main()
