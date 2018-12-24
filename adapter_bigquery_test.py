import unittest
from bigquery_testconfig import QUERY, QUERY_RESULT, SERVICE_ACC
from adapter_bigquery import AdapterBigquery


class TestAdapterBigquery(unittest.TestCase):
    def test_get_result_iter(self):
        adapter = AdapterBigquery(SERVICE_ACC)
        result = adapter.get_result_iter(QUERY)
        result = [row.values() for row in result]
        self.assertEqual(result, QUERY_RESULT)


if __name__ == '__main__':
    unittest.main()
