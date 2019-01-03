import unittest
from adapters_testconfig import *
from adapters import *
import random


class TestAdapterBigquery(unittest.TestCase):
    def setUp(self):
        rand = random.randint(0, 1000)  # For unique table name; to avoid BQ delete table bug
        self.table_adress_write = f'{DATASET}.{TABLE_NAME_W}_{rand}'
        self.table_adress_read = f'{DATASET}.{TABLE_NAME_R}'
        self.query_table_write = get_query(self.table_adress_write)
        self.query_table_read = get_query(self.table_adress_read)

    def test_get_result_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            table = adapter.get_result_table(self.query_table_read)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    def test_create_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            adapter.create_table(RESULT_TABLE, self.table_adress_write)
            table = adapter.get_result_table(self.query_table_write)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
            adapter.delete_table(self.table_adress_write)


class TestAdapterMysql(unittest.TestCase):
    def setUp(self):
        self.table_adress_write = f'{TABLE_NAME_W}'
        self.table_adress_read = f'{TABLE_NAME_R}'
        self.query_table_write = get_query(self.table_adress_write)
        self.query_table_read = get_query(self.table_adress_read)

    def test_get_result_table(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            table = adapter.get_result_table(self.query_table_read)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    def test_create_table(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            adapter.create_table(RESULT_TABLE, self.table_adress_write)
            table = adapter.get_result_table(self.query_table_write)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
            adapter.delete_table(self.table_adress_write)


class TestAdapterCsv(unittest.TestCase):
    def test_get_result_table(self):
        with AdapterCsv() as adapter:
            table = adapter.get_result_table(CSV_PATH)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    def test_create_table(self):
        with AdapterCsv() as adapter:
            adapter.create_table(RESULT_TABLE, CSV_TEMP_PATH)
            table = adapter.get_result_table(CSV_TEMP_PATH)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
            adapter.delete_table(CSV_TEMP_PATH)


class TestAdapterBigqueryToAdapterMysql(unittest.TestCase):
    def setUp(self):
        self.table_adress_write = f'{TABLE_NAME_W}'
        self.table_adress_read = f'{DATASET}.{TABLE_NAME_R}'
        self.query_table_write = get_query(self.table_adress_write)
        self.query_table_read = get_query(self.table_adress_read)

    def test_create_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter_bq:
            table = adapter_bq.get_result_table(self.query_table_read)
            with AdapterMysql(DB_CONFIG) as adapter_mysql:
                adapter_mysql.create_table(table, self.table_adress_write)
                table = adapter_mysql.get_result_table(self.query_table_write)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
                adapter_mysql.delete_table(self.table_adress_write)


class TestAdapterBigqueryToAdapterCsv(unittest.TestCase):
    def setUp(self):
        self.table_adress_read = f'{DATASET}.{TABLE_NAME_R}'
        self.query_table_read = get_query(self.table_adress_read)

    def test_create_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter_bq:
            table = adapter_bq.get_result_table(self.query_table_read)
            with AdapterCsv() as adapter_csv:
                adapter_csv.create_table(table, CSV_TEMP_PATH)
                table = adapter_csv.get_result_table(CSV_TEMP_PATH)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
                adapter_csv.delete_table(CSV_TEMP_PATH)


class TestAdapterMysqlToAdapterBigquery(unittest.TestCase):
    def setUp(self):
        rand = random.randint(0, 1000)  # For unique table name; to avoid BQ delete table bug
        self.table_adress_write = f'{DATASET}.{TABLE_NAME_W}_{rand}'
        self.table_adress_read = f'{TABLE_NAME_R}'
        self.query_table_write = get_query(self.table_adress_write)
        self.query_table_read = get_query(self.table_adress_read)

    def test_create_table(self):
        with AdapterMysql(DB_CONFIG) as adapter_mysql:
            table = adapter_mysql.get_result_table(self.query_table_read)
            with AdapterBigquery(SERVICE_ACC) as adapter_bq:
                adapter_bq.create_table(table, self.table_adress_write)
                table = adapter_bq.get_result_table(self.query_table_write)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
                adapter_bq.delete_table(self.table_adress_write)


class TestAdapterMysqlToAdapterCsv(unittest.TestCase):
    def setUp(self):
        self.table_adress_read = f'{TABLE_NAME_R}'
        self.query_table_read = get_query(self.table_adress_read)

    def test_create_table(self):
        with AdapterMysql(DB_CONFIG) as adapter_mysql:
            table = adapter_mysql.get_result_table(self.query_table_read)
            with AdapterCsv() as adapter_csv:
                adapter_csv.create_table(table, CSV_TEMP_PATH)
                table = adapter_csv.get_result_table(CSV_TEMP_PATH)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
                adapter_csv.delete_table(CSV_TEMP_PATH)


class TestAdapterCsvToAdapterBigquery(unittest.TestCase):
    def setUp(self):
        rand = random.randint(0, 1000)  # For unique table name; to avoid BQ delete table bug
        self.table_adress_write = f'{DATASET}.{TABLE_NAME_W}_{rand}'
        self.query_table_write = get_query(self.table_adress_write)

    def test_create_table(self):
        with AdapterCsv() as adapter_csv:
            table = adapter_csv.get_result_table(CSV_PATH)
            with AdapterBigquery(SERVICE_ACC) as adapter_bq:
                adapter_bq.create_table(table, self.table_adress_write)
                table = adapter_bq.get_result_table(self.query_table_write)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
                adapter_bq.delete_table(self.table_adress_write)


class TestAdapterCsvToAdapterMysql(unittest.TestCase):
    def setUp(self):
        self.table_adress_write = f'{TABLE_NAME_W}'
        self.query_table_write = get_query(self.table_adress_write)

    def test_create_table(self):
        with AdapterCsv() as adapter_csv:
            table = adapter_csv.get_result_table(CSV_PATH)
            with AdapterMysql(DB_CONFIG) as adapter_mysql:
                adapter_mysql.create_table(table, self.table_adress_write)
                table = adapter_mysql.get_result_table(self.query_table_write)
                self.assertEqual(table.schema, RESULT_TABLE.schema)
                self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)
                adapter_mysql.delete_table(self.table_adress_write)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
