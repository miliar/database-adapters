from adapter_abstract import AdapterAbstract, Table
from google.cloud import bigquery
import itertools


class AdapterBigquery(AdapterAbstract):
    BQ_TO_ADAPTER = {
        'STRING': 'STRING',
        'BYTES': 'STRING',
        'INTEGER': 'INTEGER',
        'INT64': 'INTEGER',
        'FLOAT': 'FLOAT',
        'FLOAT64': 'FLOAT',
        'BOOLEAN': 'STRING',
        'BOOL': 'STRING',
        'TIMESTAMP': 'STRING',
        'DATE': 'STRING',
        'TIME': 'STRING',
        'DATETIME': 'STRING',
        'RECORD': 'STRING',
        'STRUCT': 'STRING'
    }

    def __init__(self, service_acc):
        super().__init__()
        self.__client = bigquery.Client.from_service_account_json(service_acc)

    def get_result_table(self, query):
        query_job = self.__client.query(query)
        query_result = query_job.result()
        schema = self.__get_table_schema(query_result)
        row_iter = self.__get_row_iter(query_result)
        return Table(schema, row_iter)

    def __get_table_schema(self, query_result):
        column_names = [column.name for column in query_result.schema]
        column_types = [column.field_type for column in query_result.schema]
        column_types = self.__map_to_adapter_datatypes(column_types)
        schema = zip(column_names, column_types)
        return list(schema)

    def __map_to_adapter_datatypes(self, datatypes):
        adapter_datatypes = [self.BQ_TO_ADAPTER[datatype] for datatype in datatypes]
        return adapter_datatypes

    def __get_row_iter(self, query_result):
        for row in query_result:
            yield row.values()

    def create_table(self, table, table_adress):
        table_ref = self.__get_table_ref_from_adress(table_adress)
        table_bq = self.__create_empty_table(table_ref, table.schema)
        self.__insert_data_in_table(table_bq, table.row_iter)

    def __get_table_ref_from_adress(self, table_adress):
        dataset_id, table_id = table_adress.split('.')
        dataset_ref = self.__client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        return table_ref

    def __create_empty_table(self, table_ref, table_schema):
        schema = [bigquery.SchemaField(*column_schema) for column_schema in table_schema]
        table_bq = bigquery.Table(table_ref, schema=schema)
        table_bq = self.__client.create_table(table_bq)
        return table_bq

    def __insert_data_in_table(self, table_ref, row_iter, chunksize=1000):
        while True:
            rows = list(itertools.islice(row_iter, chunksize))
            if rows:
                self.__insert_rows(table_ref, rows)
            else:
                break

    def __insert_rows(self, table_ref, rows):
        errors = self.__client.insert_rows(table_ref, rows)
        if errors:
            self.log_exception(errors)

    def delete_table(self, table_adress):
        table_ref = self.__get_table_ref_from_adress(table_adress)
        self.__client.delete_table(table_ref)
