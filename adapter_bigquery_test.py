import unittest
from bigquery_testconfig import QUERY, QUERY_RESULT, SERVICE_ACC
from adapter_bigquery import AdapterBigquery


class TestAdapterBigquery(unittest.TestCase):
    def test_get_result_iter(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            result = adapter.get_result_iter(QUERY)
            self.assertEqual(list(result), QUERY_RESULT)


if __name__ == '__main__':
    unittest.main()
