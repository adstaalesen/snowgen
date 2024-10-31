import inquirer
from snowgen.database_objects.snowflake_database_object import SnowflakeDatabaseObject
from snowgen.database_repository.database_repository import DatabaseRepository


def create_new_schema_in(
    database_repository: DatabaseRepository, schema: str, schema_template: str
):

    schema_config = database_repository.get_schema_template(schema_template)

    def save_objects(object_type, objects):
        for obj in objects:
            snowflake_object = SnowflakeDatabaseObject(
                database=schema_config["database"],
                schema=schema,
                role=schema_config["role"],
                object_type=object_type,
                name=obj["object_name"],
                **obj,
            )
            snowflake_object.save_object(
                snowflake_objects_path=database_repository.get_snowflake_objects_path(),
                sql_template=database_repository.get_sql_template(obj["template_name"]),
                **obj,
            )

    if "file_formats" in schema_config:
        save_objects("file_formats", schema_config["file_formats"])

    if "stages" in schema_config:
        save_objects("internal_stages", schema_config["stages"])

    if "sequences" in schema_config:
        save_objects("sequences", schema_config["sequences"])

    if "tables" in schema_config.keys():
        for object in schema_config["tables"]:

            if object["generate_columns_from_template"]:

                delimiter = inquirer.prompt(
                    [
                        inquirer.Text(
                            "field_delimiter",
                            message=f"Please enter the data delimiter for {schema}",
                        ),
                    ]
                )

                tables_to_generate = (
                    database_repository.get_table_columns_from_template_files(
                        data_template_name=schema,
                        delimiter=delimiter["field_delimiter"],
                    )
                )

                for t in tables_to_generate:

                    for key in t.keys():
                        if key not in [
                            object.keys(),
                            "role",
                            "database",
                            "schema",
                            "object_name",
                            "object_type",
                        ]:
                            object[key] = t[key]

                        save_objects("tables", object)

                    # table_object = SnowflakeDatabaseObject(
                    #     role=schema_config["role"],
                    #     database=schema_config["database"],
                    #     schema=schema,
                    #     name=t["name"],
                    #     object_type="tables",
                    #     **object,
                    # )

                    # table_object.save_object(
                    #     snowflake_objects_path=database_repository.get_snowflake_objects_path(),
                    #     sql_template=database_repository.get_sql_template(
                    #         object["template_name"]
                    #     ),
                    #     **object,
                    # )

    if "dynamic_tables" in schema_config.keys():
        for object in schema_config["dynamic_tables"]:
            if object["generate_columns_from_table"]:

                dynamic_tables_to_generate = (
                    database_repository.get_dynamic_table_transformations_from_table()
                )

                for t in dynamic_tables_to_generate:

                    for key in t.keys():
                        if key not in [
                            object.keys(),
                            "role",
                            "database",
                            "schema",
                            "object_name",
                            "object_type",
                        ]:
                            object[key] = t[key]

                    dynamic_table_object = SnowflakeDatabaseObject(
                        role=schema_config["role"],
                        database=schema_config["database"],
                        schema=schema,
                        name=t["object_name"],
                        object_type="dynamic_tables",
                        **object,
                    )
                    dynamic_table_object.save_object(
                        snowflake_objects_path=database_repository.get_snowflake_objects_path(),
                        sql_template=database_repository.get_sql_template(
                            object["template_name"]
                        ),
                        **object,
                    )

    if "procedures" in schema_config.keys():
        for object in schema_config["procedures"]:
            procedure = SnowflakeDatabaseObject(
                database=schema_config["database"],
                schema=schema,
                role=schema_config["role"],
                object_type="procedures",
                name=object["object_name"],
            )

            procedure.save_object(
                snowflake_objects_path=database_repository.get_snowflake_objects_path(),
                sql_template=database_repository.get_sql_template(
                    object["template_name"]
                ),
                **object,
            )


def init(database_repository: DatabaseRepository):
    database_repository.setup()
