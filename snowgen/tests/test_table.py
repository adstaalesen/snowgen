import unittest
from snowflake_objects.table import Table
from snowflake_objects.snowflake_database_object import SnowflakeDatabaseObject


class TestTable(unittest.TestCase):
    def setUp(self):
        self.database = "test_database"
        self.schema = "test_schema"
        self.columns = ["column1", "column2"]
        self.table = Table(self.database, self.schema, self.columns)

    def test_table_inheritance(self):
        self.assertIsInstance(self.table, SnowflakeDatabaseObject)

    def test_table_initialization(self):
        self.assertEqual(self.table.schema, self.schema)
        self.assertEqual(self.table.database, self.database)
        self.assertEqual(self.table.columns, self.columns)


if __name__ == "__main__":
    unittest.main()
