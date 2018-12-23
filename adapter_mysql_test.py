import unittest
from mysql_config_test import DB_CONFIG, QUERY, QUERY_RESULT
from adapter_mysql import AdapterMysql


class TestAdapterMysql(unittest.TestCase):
    def test_get_result_iter(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            result = adapter.get_result_iter(QUERY)
            self.assertEqual(list(result), QUERY_RESULT)


if __name__ == '__main__':
    unittest.main()
