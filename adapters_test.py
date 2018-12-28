import unittest
from adapters_testconfig import *
from adapters import AdapterBigquery, AdapterMysql


class TestAdapterBigquery(unittest.TestCase):
    def test_get_result_table(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            schema, rows = adapter.get_result_table(QUERY_BQ)
            print(schema)  # needs to be tested, also for other than STRING
            self.assertEqual(list(rows), QUERY_RESULT_BQ)

    def test_create_table(self):
        pass

    def test_create_table_from_csv(self):
        pass


class TestAdapterMysql(unittest.TestCase):
    def test_get_result_table(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            schema, rows = adapter.get_result_table(QUERY_MYSQL)
            print(schema)
            self.assertEqual(list(rows), QUERY_RESULT_MYSQL)


if __name__ == '__main__':
    unittest.main()
