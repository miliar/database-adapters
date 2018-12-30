import unittest
from adapters_testconfig import *
from adapters import AdapterBigquery, AdapterMysql, AdapterCsv


class TestAdapterBigquery(unittest.TestCase):
    def test_get_result_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            schema, rows = adapter.get_result_table(QUERY_BQ)
            self.assertEqual(schema, QUERY_RESULT_SCHEMA)
            self.assertEqual(list(rows), QUERY_RESULT_BQ)

    def test_create_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            adapter.create_table(QUERY_RESULT_SCHEMA, QUERY_RESULT_BQ, 'test_data', 'test_writing')
            schema, rows = adapter.get_result_table(QUERY_BQ)
            self.assertEqual(schema, QUERY_RESULT_SCHEMA)
            self.assertEqual(list(rows), QUERY_RESULT_BQ)
            adapter.delete_table('test_data', 'test_writing')


class TestAdapterMysql(unittest.TestCase):
    def test_get_result_table(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            schema, rows = adapter.get_result_table(QUERY_MYSQL)
            self.assertEqual(schema, QUERY_RESULT_SCHEMA)
            self.assertEqual(list(rows), QUERY_RESULT_MYSQL)

    def test_create_table(self):
        pass


class TestAdapterCsv(unittest.TestCase):
    def test_get_result_table(self):
        with AdapterCsv() as adapter:
            schema, rows = adapter.get_result_table(CSV_MYSQL_PATH)
            self.assertEqual(schema, QUERY_RESULT_SCHEMA)
            self.assertEqual(list(rows), QUERY_RESULT_MYSQL)

    def test_create_table(self):
        with AdapterCsv() as adapter:
            adapter.create_table(QUERY_RESULT_SCHEMA, QUERY_RESULT_MYSQL, CSV_TEMP_PATH)
            schema, rows = adapter.get_result_table(CSV_TEMP_PATH)
            self.assertEqual(schema, QUERY_RESULT_SCHEMA)
            self.assertEqual(list(rows), QUERY_RESULT_MYSQL)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
