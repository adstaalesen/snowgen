from snowgen.database_objects.snowflake_database_object import SnowflakeDatabaseObject
from snowgen.database_repository.database_repository import DatabaseRepository


def create_new_schema_in(
    database_repository: DatabaseRepository,
    schema: str,
    schema_template: str,
    replace=False,
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

            database_repository.save_database_object(
                ddl=snowflake_object.get_ddl(
                    sql_template=database_repository.get_sql_template(
                        obj["template_name"]
                    )
                ),
                object_path=snowflake_object.generate_object_path(
                    database_repository.snowflake_objects_path
                ),
                replace=replace,
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

                tables_to_generate = (
                    database_repository.get_table_columns_from_template_files(
                        data_template_name=schema,
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

                    save_objects("dynamic_tables", object)

    if "procedures" in schema_config.keys():
        for object in schema_config["procedures"]:
            save_objects("procedures", schema_config["procedures"])


def init(database_repository: DatabaseRepository):
    database_repository.setup()
