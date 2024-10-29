snowgen setup
--> Set's up a local folder and sql_templates with template database paths, using the DatabaseRepository class

snowgen new database
--> Creates a new database in the local folder, snowflake/snowflake_objects/databases/<database_name>
--> prompts the user for database name
--> only needs pathlib and input from user

snowgen new schema
--> prompts user for database name and schema name, then creates a new schema in the local
--> prompts the user for the yml template for generating the schema

snowgen update database <database_name> schema <schema_name> object_type <object_type>
snowgen update all object_type <object_type>
--> Updates the local database with the latest schema changes from the snowflake database based on templates




