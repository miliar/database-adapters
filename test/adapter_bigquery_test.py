import unittest
import random
from test.config import *
from adapters.adapter_bigquery import AdapterBigquery


class TestAdapterBigquery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # For unique table name; to avoid 'BQ delete table bug'
        rand = random.randint(0, 1000)
        cls.adress_table_write = f'{DATASET}.{TABLE_NAME_W}_{rand}'
        cls.adress_table_read = f'{DATASET}.{TABLE_NAME_R}'
        cls.query_table_write = get_query(cls.adress_table_write)
        cls.query_table_read = get_query(cls.adress_table_read)

    def test_get_result_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            table = adapter.get_result_table(self.query_table_read)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    def test_create_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            table = Table(RESULT_TABLE.schema,
                          (row for row in RESULT_TABLE.row_iter))
            adapter.create_table(table, self.adress_table_write)
            table = adapter.get_result_table(self.query_table_write)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            adapter.delete_table(cls.adress_table_write)
