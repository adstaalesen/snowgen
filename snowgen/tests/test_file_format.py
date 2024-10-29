import unittest

from snowflake_objects.file_format import (
    FileFormat,
    CSVFormat,
    JSONFormat,
    AVROFormat,
    ORCFormat,
    PARQUETFormat,
    XMLFormat,
)


class TestFileFormat(unittest.TestCase):
    def test_file_format_initialization(self):
        file_format = FileFormat("CSV", compression="GZIP", encoding="UTF16")
        self.assertEqual(file_format.file_type, "CSV")
        self.assertEqual(file_format.compression, "GZIP")
        self.assertEqual(file_format.encoding, "UTF16")

    def test_csv_format_initialization(self):
        csv_format = CSVFormat(compression="GZIP", parse_header=True)
        self.assertEqual(csv_format.file_type, "CSV")
        self.assertEqual(csv_format.compression, "GZIP")
        self.assertTrue(csv_format.parse_header)
        self.assertEqual(csv_format.encoding, "UTF8")  # default value

    def test_json_format_initialization(self):
        json_format = JSONFormat(compression="GZIP", allow_duplicate=True)
        self.assertEqual(json_format.file_type, "JSON")
        self.assertEqual(json_format.compression, "GZIP")
        self.assertTrue(json_format.allow_duplicate)
        self.assertFalse(json_format.ignore_utf8_errors)  # default value

    def test_avro_format_initialization(self):
        avro_format = AVROFormat(compression="SNAPPY")
        self.assertEqual(avro_format.file_type, "AVRO")
        self.assertEqual(avro_format.compression, "SNAPPY")
        self.assertFalse(avro_format.trim_space)  # default value

    def test_orc_format_initialization(self):
        orc_format = ORCFormat(trim_space=True)
        self.assertEqual(orc_format.file_type, "ORC")
        self.assertTrue(orc_format.trim_space)
        self.assertEqual(orc_format.null_if, [])  # default value

    def test_parquet_format_initialization(self):
        parquet_format = PARQUETFormat(snappy_compression=True, binary_as_text=True)
        self.assertEqual(parquet_format.file_type, "PARQUET")
        self.assertTrue(parquet_format.snappy_compression)
        self.assertTrue(parquet_format.binary_as_text)
        self.assertEqual(parquet_format.compression, "AUTO")  # default value

    def test_xml_format_initialization(self):
        xml_format = XMLFormat(compression="GZIP", preserve_space=True)
        self.assertEqual(xml_format.file_type, "XML")
        self.assertEqual(xml_format.compression, "GZIP")
        self.assertTrue(xml_format.preserve_space)
        self.assertFalse(xml_format.strip_outer_element)  # default value


if __name__ == "__main__":
    unittest.main()
