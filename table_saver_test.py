from table_saver import TableSaver
from adapters_testconfig import *
from adapters import AdapterBigquery, AdapterMysql
import unittest
import csv


class TestTableSaver(unittest.TestCase):
    def setUp(self):
        self.save_path = SAVE_PATH + '/test.csv'
        with open(SAVE_PATH + '/test_bigquery.csv', newline='') as bq_csv:
            self.result_bigquery = list(csv.reader(bq_csv, delimiter=' ', quotechar='|'))
        with open(SAVE_PATH + '/test_mysql.csv', newline='') as mysql_csv:
            self.result_mysql = list(csv.reader(mysql_csv, delimiter=' ', quotechar='|'))

    def test_result_to_csv_with_bigquery(self):
        with AdapterBigquery(SERVICE_ACC) as adapter:
            TableSaver(QUERY_BQ, adapter).result_to_csv(self.save_path)
        with open(self.save_path, newline='') as result_csv:
            result = list(csv.reader(result_csv, delimiter=' ', quotechar='|'))

        self.assertEqual(result, self.result_bigquery)

    def test_result_to_csv_with_mysql(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            TableSaver(QUERY_MYSQL, adapter).result_to_csv(self.save_path)
        with open(self.save_path, newline='') as result_csv:
            result = list(csv.reader(result_csv, delimiter=' ', quotechar='|'))

        self.assertEqual(result, self.result_mysql)


if __name__ == '__main__':
    # unittest.main()
    with AdapterMysql(DB_CONFIG) as adapter1:
        with AdapterBigquery(SERVICE_ACC) as adapter2:
            table = TableSaver(QUERY_MYSQL, adapter1)
            table.save_result_in(adapter2, dataset_id='test_data', table_id='test_writing2')
