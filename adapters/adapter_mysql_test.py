import unittest
from adapters_testconfig import *
from adapter_mysql import AdapterMysql


class TestAdapterMysql(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.adress_table_write = f'{TABLE_NAME_W}'
        cls.adress_table_read = f'{TABLE_NAME_R}'
        cls.query_table_write = get_query(cls.adress_table_write)
        cls.query_table_read = get_query(cls.adress_table_read)

    def test_get_result_table(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            table = adapter.get_result_table(self.query_table_read)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    def test_create_table(self):
        with AdapterMysql(DB_CONFIG) as adapter:
            table = Table(RESULT_TABLE.schema, (row for row in RESULT_TABLE.row_iter))
            adapter.create_table(table, self.adress_table_write)
            table = adapter.get_result_table(self.query_table_write)
            self.assertEqual(table.schema, RESULT_TABLE.schema)
            self.assertEqual(list(table.row_iter), RESULT_TABLE.row_iter)

    @classmethod
    def tearDownClass(cls):
        with AdapterMysql(DB_CONFIG) as adapter:
            adapter.delete_table(cls.adress_table_write)


if __name__ == '__main__':
    unittest.main(warnings='ignore')
