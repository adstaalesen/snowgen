from pathlib import Path
import re
import yaml
import inquirer


class DatabaseRepository:

    SQL_TEMPLATES_FOLDER_NAME = "sql_templates"

    _template_setup = [
        {
            "object": "file_formats",
            "file_name": "file_format.sql",
            "sql_statement": "CREATE FILE FORMAT IF NOT EXISTS {{name}}",
        },
        {
            "object": "internal_stages",
            "file_name": "internal_stage.sql",
            "sql_statement": "CREATE STAGE IF NOT EXISTS {{name}}",
        },
        {
            "object": "tables",
            "file_name": "table.sql",
            "sql_statement": "CREATE TABLE IF NOT EXISTS {{name}}",
        },
        {
            "object": "procedures",
            "file_name": "procedure.sql",
            "sql_statement": "CREATE PROCEDURE IF NOT EXISTS {{name}}",
        },
    ]

    def __init__(self, snowflake_path="snowflake"):
        self.snowflake_path = snowflake_path
        self.base_path = Path("./")
        self.snowflake_objects_path = (
            self.base_path / snowflake_path / "snowflake_objects"
        )

    def setup(self):
        """
        Create the folder structure:
        ./snowflake/snowflake_objects/databases/template_db/schemas/template_schema/
        """
        db_path = (
            self.snowflake_objects_path
            / "databases"
            / "template_db"
            / "schemas"
            / "template_schema"
        )

        for template in self._template_setup:
            object_path = db_path / template["object"]
            object_path.mkdir(parents=True, exist_ok=True)
            file_path = object_path / template["file_name"]
            with open(file_path, "w") as file:
                file.write(template["sql_statement"])

        # Create the directory structure
        print(f"Created directory structure")

    def prompt_user_for_source(self):
        database = inquirer.prompt(
            [
                inquirer.List(
                    "database",
                    message="Select DATABASE that holds tables to transform",
                    choices=self.get_all_databases(),
                )
            ]
        )["database"]

        schema = inquirer.prompt(
            [
                inquirer.List(
                    "schema",
                    message="Select SCHEMA that holds tables to transform",
                    choices=self.get_all_schemas(database),
                )
            ]
        )["schema"]

        return database, schema

    def prompt_user_for_delimiter(self, schema):
        return inquirer.prompt(
            [
                inquirer.Text(
                    "field_delimiter",
                    message=f"Enter the field delimiter used in the data files for {schema}",
                    default=",",
                )
            ]
        )["field_delimiter"]

    """ Methods for finding and reading YAML templates """

    def load_yaml_file(self, yaml_file):
        with open(yaml_file, "r") as file:
            config = yaml.safe_load(file)
        return config

    def get_schema_templates_yaml(self):
        return self.load_yaml_file(
            self.base_path / "templates" / "schema_templates" / "schemas.yaml"
        )

    def get_schema_templates(self):
        schema_config = self.get_schema_templates_yaml()
        return [schema_config["name"] for schema_config in schema_config["schemas"]]

    def get_schema_template(self, template_name):
        schema_config = self.get_schema_templates_yaml()
        for schema_template in schema_config["schemas"]:
            if schema_template["name"] == template_name:
                return schema_template

    """ Methods for finding and reading SQL templates """

    def _find_folder_path(self, folder_name=SQL_TEMPLATES_FOLDER_NAME):
        """
        Search for the path to the specified folder starting from the given path.
        """
        for path in self.base_path.rglob("*"):
            if path.is_dir() and path.name == folder_name:
                return str(path)
        return None

    def get_sql_template(self, template_name):
        """
        Get the path to the specified folder and return the path to the SQL templates folder.
        """
        folder_path = self._find_folder_path()

        if folder_path:
            with Path(folder_path) / template_name as file:
                return file.read_text()
        return None

    def save_database_object(self, ddl: str, object_path: str, replace=False):
        """
        Save the database object to the specified path.
        """

        object_path.parent.mkdir(parents=True, exist_ok=True)

        if replace:
            with open(object_path, "w") as file:
                file.write(ddl)
        else:
            if not Path(object_path).exists():
                with open(object_path, "w") as file:
                    file.write(ddl)

    def extract_filename_parts(self, filename):
        """
        Extract parts of a filename that follows the pattern <filename>_<date>.<file_type>.
        """
        pattern = (
            r"(?P<filename>.+)_(?P<date>\d{4}-\d{2}-\d{2}|\d{8})\.(?P<file_type>.+)"
        )
        match = re.match(pattern, filename)
        if match:
            return match.groupdict()
        return None

    def get_all_databases(self):
        return [p.name for p in (self.snowflake_objects_path / "databases").iterdir()]

    def get_all_schemas(self, database):
        return [
            p.name
            for p in (
                self.snowflake_objects_path / "databases" / database / "schemas"
            ).iterdir()
        ]

    def get_table_columns_from_template_files(self, data_template_name):
        folder_path = self._find_folder_path(folder_name="data_templates")
        data_path = (Path(folder_path) / data_template_name).resolve()
        files_in_data_path = [f for f in data_path.glob("*.*")]

        tables = []

        delimiter = self.prompt_user_for_delimiter(data_template_name)

        for file in files_in_data_path:
            with open(file, "r") as f:
                header_row = f.readline().strip()

            tables.append(
                {
                    "columns": header_row.split(delimiter),
                    "object_name": self.extract_filename_parts(file.name)["filename"],
                    "comment": f"SQL generated using file {file.name}",
                }
            )

        return tables

    def get_columns_from_table_definition(self, table_definition: str) -> list[str]:
        """Extract substring between double quotes and return as a list of columns."""
        pattern = r'"([^"]*)"'
        columns = re.findall(pattern, table_definition)
        return columns

    def parse_dynamic_table_definition(self, table_definition):
        columns = self.get_columns_from_table_definition(table_definition)
        source_database = re.findall(r"USE DATABASE ([\w_{}]+)", table_definition)
        source_schema = re.findall(r"USE SCHEMA (\w+)", table_definition)

        table_name_patterns = [
            r"CREATE OR REPLACE TABLE (\w+)",
            r"CREATE TABLE (\w+)",
            r"CREATE OR ALTER TABLE (\w+)",
        ]

        source_object = None
        for pattern in table_name_patterns:
            match = re.findall(pattern, table_definition)
            if match:
                source_object = match[0]
                break

        return {
            "columns": columns,
            "source_database": source_database[0] if source_database else None,
            "source_schema": source_schema[0] if source_schema else None,
            "source_object": source_object,
        }

    def get_dynamic_table_transformations_from_table(self):

        database, schema = self.prompt_user_for_source()

        tables_path = (
            self.snowflake_objects_path
            / "databases"
            / database
            / "schemas"
            / schema
            / "tables"
        ).resolve()

        files_in_tables_path = [f for f in tables_path.glob("*.sql")]

        dynamic_tables = []

        for file in files_in_tables_path:
            with open(file, "r") as f:
                table_definition = f.read().strip()

            table_info = self.parse_dynamic_table_definition(table_definition)
            table_info["object_name"] = file.name.split(".")[0].lower()
            dynamic_tables.append(table_info)

        return dynamic_tables

    def get_snowflake_objects_path(self):
        return self.snowflake_objects_path


# Example usage
if __name__ == "__main__":
    databases = []  # Replace with actual database objects
    repo = DatabaseRepository(name="test_repo", databases=databases)
    repo.setup()
