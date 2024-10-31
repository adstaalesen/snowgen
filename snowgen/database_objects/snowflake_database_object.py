from pathlib import Path


class SnowflakeDatabaseObject:

    def __init__(
        self,
        role: str,
        database: str,
        schema: str,
        object_name: str,
        object_type: str,
        **kwargs,
    ):
        self.role = role
        self.database = database
        self.schema = schema
        self.object_name = object_name.lower()
        self.object_type = object_type
        self.kwargs = kwargs
        self.pattern = []
        self.env = "env"

        if "columns" in kwargs and object_type == "tables":
            self.columns = kwargs["columns"]
            self.table_columns = self.format_table_columns(kwargs["columns"])
        elif object_type == "dynamic_tables":
            if "pattern" in kwargs:
                self.pattern = kwargs["pattern"]
            self.columns = kwargs["columns"]
            self.formatted_transformations = self.format_transformations()
            self.source_database = kwargs["source_database"]
            self.source_schema = kwargs["source_schema"]
            self.source_object = kwargs["source_object"]

    def __str__(self):
        return f"{self.role}.{self.database}.{self.schema}.{self.object_name}"

    def to_dict(self):
        return self.__dict__

    def generate_object_path(self, snowflake_objects_path):
        if isinstance(snowflake_objects_path, dict):
            raise TypeError(
                "snowflake_objects_path should be a string or Path, not a dict"
            )

        return (
            Path(snowflake_objects_path)
            / "databases"
            / self.database
            / "schemas"
            / self.schema
            / self.object_type
            / self.generate_filename()
        )

    def generate_filename(self):

        prefix = self.kwargs.get("prefix", "")
        suffix = self.kwargs.get("suffix", "")
        return (
            "_".join(
                [part for part in [prefix, self.object_name, suffix] if part]
            ).lower()
            + ".sql"
        )

    def get_ddl(self, sql_template, **kwargs):

        replacements = {**self.__dict__, **kwargs}
        prefix = self.kwargs.get("prefix", "")
        suffix = self.kwargs.get("suffix", "")
        replacements["name"] = "_".join(
            [part for part in [prefix, self.object_name, suffix] if part]
        )

        ddl = sql_template.format(**replacements)

        return ddl

    def get_reserved_keywords(self):
        return [
            "ACCOUNT",
            "ALL",
            "ALTER",
            "AND",
            "ANY",
            "AS",
            "BETWEEN",
            "BY",
            "CASE",
            "CAST",
            "CHECK",
            "COLUMN",
            "CONNECT",
            "CONNECTION",
            "CONSTRAINT",
            "CREATE",
            "CROSS",
            "CURRENT",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_USER",
            "DATABASE",
            "DELETE",
            "DISTINCT",
            "DROP",
            "ELSE",
            "EXISTS",
            "FALSE",
            "FOLLOWING",
            "FOR",
            "FROM",
            "FULL",
            "GRANT",
            "GROUP",
            "GSCLUSTER",
            "HAVING",
            "ILIKE",
            "IN",
            "INCREMENT",
            "INNER",
            "INSERT",
            "INTERSECT",
            "INTO",
            "IS",
            "ISSUE",
            "JOIN",
            "LATERAL",
            "LEFT",
            "LIKE",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "MINUS",
            "NATURAL",
            "NOT",
            "NULL",
            "OF",
            "ON",
            "OR",
            "ORDER",
            "ORGANIZATION",
            "QUALIFY",
            "REGEXP",
            "REVOKE",
            "RIGHT",
            "RLIKE",
            "ROW",
            "ROWS",
            "SAMPLE",
            "SCHEMA",
            "SELECT",
            "SET",
            "SOME",
            "START",
            "TABLE",
            "TABLESAMPLE",
            "THEN",
            "TO",
            "TRIGGER",
            "TRUE",
            "TRY_CAST",
            "UNION",
            "UNIQUE",
            "UPDATE",
            "USING",
            "VALUES",
            "VIEW",
            "WHEN",
            "WHENEVER",
            "WHERE",
            "WITH",
        ]

    def check_column_validity(self, column, reserved_keywords):
        comment = ""
        if column in reserved_keywords:
            comment += " -- This column name is not allowed"
        if isinstance(column[0], int):
            comment += " -- Column name cannot start with a number"
        if len(column) > 128:
            comment += " -- Column name should not exceed 128 characters"
        return comment

    def format_table_columns(self, columns):
        reserved_keywords = self.get_reserved_keywords()
        if isinstance(columns, list):
            formatted_columns = ",\n    ".join(
                [
                    (
                        f'"{col}" VARCHAR'
                        + self.check_column_validity(
                            column=col, reserved_keywords=reserved_keywords
                        )
                    )
                    for col in columns
                ]
            )
        elif isinstance(columns, dict):
            formatted_columns = ",\n    ".join(
                [
                    (
                        f'"{col}" {dtype}'
                        + self.check_column_validity(
                            column=col, reserved_keywords=reserved_keywords
                        )
                    )
                    for col, dtype in columns.items()
                ]
            )
        else:
            raise ValueError("Columns should be either a list or a dictionary")
        return formatted_columns

    def format_transformations(self):
        """
        Format the transformation for each column using the provided pattern.

        Args:
            pattern (str): The pattern to use for formatting. Use `{col}` as a placeholder for the column name.

        Returns:
            list: A list of formatted transformation strings.
        """
        reserved_keywords = self.get_reserved_keywords()
        all_transformations = []

        if (
            self.pattern
            and isinstance(self.columns, list)
            and all(isinstance(t, str) for t in self.columns)
        ):
            for column in self.columns:
                all_transformations.append(
                    self.pattern.format(column_name=column)
                    + self.check_column_validity(
                        column=column,
                        reserved_keywords=reserved_keywords,
                    )
                )

        else:
            raise ValueError(
                "Column transformations should be either a list of columns"
            )

        return ",\n    ".join(all_transformations)
