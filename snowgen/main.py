from snowgen.database_objects.snowflake_database_object import SnowflakeDatabaseObject
from snowgen.database_repository.database_repository import DatabaseRepository


def save_objects(
    database_repository: DatabaseRepository,
    schema_config,
    schema: str,
    object_type: str,
    objects,
    replace,
):
    for obj in objects:
        snowflake_object = SnowflakeDatabaseObject(
            role=schema_config["role"],
            database=schema_config["database"],
            schema=schema,
            object_type=object_type,
            **obj,
        )

        ddl = snowflake_object.get_ddl(
            sql_template=database_repository.get_sql_template(
                template_name=obj["template_name"]
            ),
            **obj,
        )

        object_path = snowflake_object.generate_object_path(
            database_repository.snowflake_objects_path
        ).resolve()

        database_repository.save_database_object(ddl, object_path, replace=replace)


def create_schema_in(
    database_repository: DatabaseRepository,
    schema: str,
    schema_template: str,
    replace=False,
):

    schema_config = database_repository.get_schema_template(schema_template)

    if "schema_definition" in schema_config:
        save_objects(
            database_repository,
            schema_config,
            schema,
            "schema_definition",
            schema_config["schema_definition"],
            replace,
        )

    if "file_formats" in schema_config:
        save_objects(
            database_repository,
            schema_config,
            schema,
            "file_formats",
            schema_config["file_formats"],
            replace,
        )

    if "stages" in schema_config:
        save_objects(
            database_repository,
            schema_config,
            schema,
            "internal_stages",
            schema_config["stages"],
            replace,
        )

    if "sequences" in schema_config:
        save_objects(
            database_repository,
            schema_config,
            schema,
            "sequences",
            schema_config["sequences"],
            replace,
        )

    if "tables" in schema_config.keys():

        for object in schema_config["tables"]:

            tables_to_generate = []

            if object["generate_columns_from_template"]:

                tables_to_generate = (
                    database_repository.get_table_columns_from_template_files(
                        template_files_name=schema,
                    )
                )

                for t in tables_to_generate:

                    for key in object.keys():
                        if key not in [
                            t.keys(),
                            "database",
                            "schema",
                            "role",
                            "object_type",
                        ]:
                            t[key] = object[key]

                save_objects(
                    database_repository,
                    schema_config,
                    schema,
                    "tables",
                    tables_to_generate,
                    replace,
                )
            else:
                save_objects(
                    database_repository,
                    schema_config,
                    schema,
                    "tables",
                    [object],
                    replace,
                )

    if "dynamic_tables" in schema_config.keys():
        for object in schema_config["dynamic_tables"]:
            if object["generate_columns_from_table"]:

                dynamic_tables_to_generate = (
                    database_repository.get_dynamic_table_transformations_from_table()
                )

                for t in dynamic_tables_to_generate:

                    for key in object.keys():
                        if key not in [
                            t.keys(),
                            "database",
                            "schema",
                            "role",
                            "object_type",
                        ]:
                            t[key] = object[key]

                save_objects(
                    database_repository,
                    schema_config,
                    schema,
                    "dynamic_tables",
                    dynamic_tables_to_generate,
                    replace,
                )
            else:
                save_objects(
                    database_repository,
                    schema_config,
                    schema,
                    "dynamic_tables",
                    [object],
                    replace,
                )

    if "procedures" in schema_config.keys():
        for object in schema_config["procedures"]:
            save_objects(
                database_repository,
                schema_config,
                schema,
                "procedures",
                schema_config["procedures"],
                replace,
            )


def init(database_repository: DatabaseRepository):
    database_repository.setup()
