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
        pass


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
        pass

    def test_create_table(self):
        pass


if __name__ == '__main__':
    unittest.main()
