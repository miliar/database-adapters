import unittest
from test.config import *
from adapters.adapter_csv import AdapterCsv


class TestAdapterCsv(unittest.TestCase):
    def test_get_result_table(self):
        with AdapterCsv() as adapter:
            table = adapter.get_result_table(CSV_PATH)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    def test_create_table(self):
        with AdapterCsv() as adapter:
            table = Table(RESULT_TABLE.schema,
                          (row for row in RESULT_TABLE.row_iter))
            adapter.create_table(table, CSV_TEMP_PATH)
            table = adapter.get_result_table(CSV_TEMP_PATH)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterCsv() as adapter:
            adapter.delete_table(CSV_TEMP_PATH)
