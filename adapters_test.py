import unittest
from adapters_testconfig import *
from adapters import AdapterBigquery, AdapterMysql


class TestAdapterBigquery(unittest.TestCase):
    def test_get_result_iter(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            result = adapter.get_result_iter(QUERY_BQ)
            self.assertEqual(list(result), QUERY_RESULT_BQ)


class TestAdapterMysql(unittest.TestCase):
    def test_get_result_iter(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            result = adapter.get_result_iter(QUERY_MYSQL)
            self.assertEqual(list(result), QUERY_RESULT_MYSQL)


if __name__ == '__main__':
    unittest.main()
