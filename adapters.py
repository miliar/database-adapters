from abc import ABC, abstractmethod
from google.cloud import bigquery
import mysql.connector
from mysql.connector import FieldType
import logging
import traceback as tb
import csv


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

    def log_exception(self, exc):
        self.__logger.error(exc)

    @abstractmethod
    def get_result_table(self, *args):
        """ Should return table_schema, row_iter. """

    @abstractmethod
    def create_table(self, table_schema, row_iter, *args):
        """ Should create Table from Source: table_schema and row_iter.
        Supports BQ datatypes """


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
        if errors != []:
            self.log_exception(errors)

    def delete_table(self, dataset_id, table_id):
        dataset_ref = self.__client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        self.__client.delete_table(table_ref)


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
        mysql_to_adapter = {
            'DECIMAL': 'FLOAT',
            'TINY': 'INTEGER',
            'SHORT': 'INTEGER',
            'LONG': 'INTEGER',
            'FLOAT': 'FLOAT',
            'DOUBLE': 'FLOAT',
            'NULL': 'INTEGER',
            'TIMESTAMP': 'TIMESTAMP',
            'LONGLONG': 'INTEGER',
            'INT24': 'INTEGER',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'DATETIME': 'DATETIME',
            'YEAR': 'INTEGER',
            'NEWDATE': 'DATE',
            'VARCHAR': 'STRING',
            'BIT': 'INTEGER',
            'JSON': 'STRUCT',
            'NEWDECIMAL': 'INTEGER',
            'ENUM': 'STRING',
            'SET': 'STRING',
            'TINY_BLOB': 'BYTES',
            'MEDIUM_BLOB': 'BYTES',
            'LONG_BLOB': 'BYTES',
            'BLOB': 'BYTES',
            'VAR_STRING': 'STRING',
            'STRING': 'STRING',
            'GEOMETRY': 'GEOMETRY'
        }

        adapter_datatypes = [mysql_to_adapter[FieldType.get_info(d)] for d in datatypes]
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
        raise NotImplementedError('Create Table in Mysql not implemented')


class AdapterCsv(AdapterAbstract):
    def __init__(self):
        super().__init__()

    def create_table(self, table_schema, row_iter, file_name):
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(table_schema)
            for row in row_iter:
                writer.writerow(row)

    def get_result_table(self, file_name):
        with open(file_name, newline='') as csvfile:
            reader = csv.reader(csvfile)
            table_schema = [eval(item) for item in next(reader)]
            row_iter = [row for row in reader] # make generator
            return table_schema, row_iter
