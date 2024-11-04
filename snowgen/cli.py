import click
import inquirer
from .main import create_schema_in, init  # Import the function you want to trigger
from snowgen.database_repository.database_repository import DatabaseRepository


@click.group()
def cli():
    pass


@cli.command(name="new-database")
def new_database():
    """Create a new database."""
    click.echo("New database created successfully.")


@cli.command(name="generate-schema")
def create_schema_command():
    """Create a new schema."""

    database_repository = DatabaseRepository()

    schema = inquirer.prompt(
        [
            inquirer.Text("schema_name", message="Please enter the schema name"),
        ]
    )

    template = inquirer.prompt(
        [
            inquirer.List(
                "schema_template",
                message="Please choose a schema template",
                choices=database_repository.get_available_schema_templates(),
            ),
        ]
    )

    database = database_repository.get_schema_template(template["schema_template"])[
        "database"
    ]

    action = "created"

    if schema["schema_name"] in database_repository.get_all_schemas(database):

        click.echo("Schema already exists.")

        schema_exists_decision = inquirer.prompt(
            [
                inquirer.List(
                    "choice",
                    message="Do you want to regenerate or update the schema?",
                    choices=["Regenerate", "Update"],
                ),
            ]
        )

        if schema_exists_decision["choice"] == "Regenerate":
            database_repository.delete_schema(database, schema["schema_name"])
            action = "regenerated"
        else:
            action = "updated"

    create_schema_in(
        database_repository,
        schema["schema_name"],
        template["schema_template"],
        replace=False,
    )

    click.echo(f"Schema {action} successfully.")


@cli.command(name="init")
def init_repository():
    """Create a new database."""
    database_repository = DatabaseRepository()
    init(database_repository)
    click.echo("New project set up and completed.")


if __name__ == "__main__":
    cli()


# snowgen setup
# --> Set's up a local folder and sql_templates with template database paths, using the DatabaseRepository class


# snowgen new database
# --> Creates a new database in the local folder, snowflake/snowflake_objects/databases/<database_name>
# --> prompts the user for database name
# --> only needs pathlib and input from user

# snowgen new schema
# --> prompts user for database name and schema name, then creates a new schema in the local
# --> prompts the user for the yml template for generating the schema

# snowgen update database <database_name> schema <schema_name> object_type <object_type>
# snowgen update all object_type <object_type>
# --> Updates the local database with the latest schema changes from the snowflake database based on templates
