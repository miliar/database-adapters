import unittest
from test_config import *
from adapter_bigquery import AdapterBigquery
from adapter_mysql import AdapterMysql
from adapter_csv import AdapterCsv
import random


class TestAdapterBigqueryToAdapterMysql(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.adress_table_write = f'{TABLE_NAME_W}'
        cls.adress_table_read = f'{DATASET}.{TABLE_NAME_R}'
        cls.query_table_write = get_query(cls.adress_table_write)
        cls.query_table_read = get_query(cls.adress_table_read)

    def test_create_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter_bq:
            table = adapter_bq.get_result_table(self.query_table_read)
            with AdapterMysql(DB_CONFIG) as adapter_mysql:
                adapter_mysql.create_table(table, self.adress_table_write)
                table = adapter_mysql.get_result_table(self.query_table_write)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterMysql(DB_CONFIG) as adapter:
            adapter.delete_table(cls.adress_table_write)


class TestAdapterBigqueryToAdapterCsv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.adress_table_read = f'{DATASET}.{TABLE_NAME_R}'
        cls.query_table_read = get_query(cls.adress_table_read)

    def test_create_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter_bq:
            table = adapter_bq.get_result_table(self.query_table_read)
            with AdapterCsv() as adapter_csv:
                adapter_csv.create_table(table, CSV_TEMP_PATH)
                table = adapter_csv.get_result_table(CSV_TEMP_PATH)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterCsv() as adapter:
            adapter.delete_table(CSV_TEMP_PATH)


class TestAdapterMysqlToAdapterBigquery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rand = random.randint(0, 1000)  # For unique table name; to avoid BQ delete table bug
        cls.adress_table_write = f'{DATASET}.{TABLE_NAME_W}_{rand}'
        cls.adress_table_read = f'{TABLE_NAME_R}'
        cls.query_table_write = get_query(cls.adress_table_write)
        cls.query_table_read = get_query(cls.adress_table_read)

    def test_create_table(self):
        with AdapterMysql(DB_CONFIG) as adapter_mysql:
            table = adapter_mysql.get_result_table(self.query_table_read)
            with AdapterBigquery(SERVICE_ACC) as adapter_bq:
                adapter_bq.create_table(table, self.adress_table_write)
                table = adapter_bq.get_result_table(self.query_table_write)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            adapter.delete_table(cls.adress_table_write)


class TestAdapterMysqlToAdapterCsv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.adress_table_read = f'{TABLE_NAME_R}'
        cls.query_table_read = get_query(cls.adress_table_read)

    def test_create_table(self):
        with AdapterMysql(DB_CONFIG) as adapter_mysql:
            table = adapter_mysql.get_result_table(self.query_table_read)
            with AdapterCsv() as adapter_csv:
                adapter_csv.create_table(table, CSV_TEMP_PATH)
                table = adapter_csv.get_result_table(CSV_TEMP_PATH)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterCsv() as adapter:
            adapter.delete_table(CSV_TEMP_PATH)


class TestAdapterCsvToAdapterBigquery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rand = random.randint(0, 1000)  # For unique table name; to avoid BQ delete table bug
        cls.adress_table_write = f'{DATASET}.{TABLE_NAME_W}_{rand}'
        cls.query_table_write = get_query(cls.adress_table_write)

    def test_create_table(self):
        with AdapterCsv() as adapter_csv:
            table = adapter_csv.get_result_table(CSV_PATH)
            with AdapterBigquery(SERVICE_ACC) as adapter_bq:
                adapter_bq.create_table(table, self.adress_table_write)
                table = adapter_bq.get_result_table(self.query_table_write)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            adapter.delete_table(cls.adress_table_write)


class TestAdapterCsvToAdapterMysql(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.adress_table_write = f'{TABLE_NAME_W}'
        cls.query_table_write = get_query(cls.adress_table_write)

    def test_create_table(self):
        with AdapterCsv() as adapter_csv:
            table = adapter_csv.get_result_table(CSV_PATH)
            with AdapterMysql(DB_CONFIG) as adapter_mysql:
                adapter_mysql.create_table(table, self.adress_table_write)
                table = adapter_mysql.get_result_table(self.query_table_write)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterMysql(DB_CONFIG) as adapter:
            adapter.delete_table(cls.adress_table_write)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
