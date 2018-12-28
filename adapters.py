from abc import ABC, abstractmethod
from google.cloud import bigquery
import mysql.connector
from mysql.connector import FieldType
import logging
import traceback as tb


class AdapterAbstract(ABC):
    def __init__(self):
        super().__init__()
        self.__logger = self.__get_logger()

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.__handle_exception(*exception)

    def __get_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.ERROR)
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def __handle_exception(self, exc_type, exc_value, traceback):
        if traceback:
            exception = tb.format_exception(exc_type, exc_value, traceback)
            self.__logger.error(exception)

    @abstractmethod
    def get_result_table(self, query):
        """ Should return table_schema, row_iter """

    @abstractmethod
    def create_table(self, table_schema, row_iter, dataset_id, table_id):
        """ Creates table at dataset_id.table_id. Source: table_schema and row_iter """


class AdapterBigquery(AdapterAbstract):
    def __init__(self, service_acc):
        super().__init__()
        self.__client = bigquery.Client.from_service_account_json(service_acc)

    def get_result_table(self, query):
        query_job = self.__client.query(query)
        query_result = query_job.result()
        table_schema = self.__get_table_schema(query_result)
        row_iter = self.__get_row_iter(query_result)
        return table_schema, row_iter

    def __get_table_schema(self, query_result):
        schema = [(column.name, column.field_type) for column in query_result.schema]
        return schema

    def __get_row_iter(self, query_result):
        for row in query_result:
            yield row.values()

    def create_table(self, table_schema, row_iter, dataset_id, table_id):
        dataset_ref = self.__client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        schema = [bigquery.SchemaField(*column_schema) for column_schema in table_schema]
        table = bigquery.Table(table_ref, schema=schema)
        table = self.__client.create_table(table)
        table = self.__client.get_table(table_ref)

        errors = self.__client.insert_rows(table, list(row_iter))
        print(errors)  # needs error handling

    def create_table_from_csv(self, csv, dataset_id, table_id):
        dataset_ref = self.__client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        with open(csv, 'rb') as source_file:
            job = self.__client.load_table_from_file(
                source_file,
                table_ref,
                location='US',
                job_config=job_config)

        job.result()


class AdapterMysql(AdapterAbstract):
    def __init__(self, db_config):
        super().__init__()
        self.__db_config = db_config

    def __enter__(self):
        self.__connection = mysql.connector.connect(**self.__db_config)
        self.__cursor = self.__connection.cursor()
        return self

    def __exit__(self, *exception):
        super().__exit__(*exception)
        self.__cursor.close()
        self.__connection.close()

    def get_result_table(self, query):
        self.__cursor.execute(query)
        table_schema = self.__get_table_schema()
        row_iter = self.__get_row_iter()
        return table_schema, row_iter

    def __get_table_schema(self):
        column_names = [column[0] for column in self.__cursor.description]
        column_types = [column[1] for column in self.__cursor.description]
        column_types = self.__map_to_adapter_datatypes(column_types)
        schema = zip(column_names, column_types)
        return list(schema)

    def __map_to_adapter_datatypes(self, datatypes):
        # not accurate, needs testing
        adapter_datatypes = []
        for datatype in datatypes:
            if datatype in FieldType.get_string_types():
                adapter_datatypes.append('STRING')
            elif datatype in FieldType.get_number_types():
                adapter_datatypes.append('FLOAT')
            elif datatype in FieldType.get_timestamp_types():
                adapter_datatypes.append('TIMESTAMP')
            else:
                adapter_datatypes.append('STRUCT')

        return adapter_datatypes

    def __get_row_iter(self, chunksize=10):
        while True:
            rows = self.__cursor.fetchmany(chunksize)
            if rows:
                for row in rows:
                    yield row
            else:
                break

    def create_table(self, table_schema, row_iter, dataset_id, table_id):
        pass
