from abc import ABC, abstractmethod
from collections import namedtuple, defaultdict
from google.cloud import bigquery
import mysql.connector
from mysql.connector import FieldType
import logging
import traceback as tb
import csv
import re
import os
import itertools


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
        """ Should return a Table instance"""

    @abstractmethod
    def create_table(self, table, *args):
        """ Should create a table from a Table instance """

    @abstractmethod
    def delete_table(self, *args):
        """Should delete a table"""


Table = namedtuple('Table', ['schema', 'row_iter'])


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


class AdapterMysql(AdapterAbstract):
    MYSQL_TO_ADAPTER = {
        'DECIMAL': 'FLOAT',
        'TINY': 'INTEGER',
        'SHORT': 'INTEGER',
        'LONG': 'INTEGER',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'FLOAT',
        'NULL': 'INTEGER',
        'TIMESTAMP': 'STRING',
        'LONGLONG': 'INTEGER',
        'INT24': 'INTEGER',
        'DATE': 'STRING',
        'TIME': 'STRING',
        'DATETIME': 'STRING',
        'YEAR': 'INTEGER',
        'NEWDATE': 'STRING',
        'VARCHAR': 'STRING',
        'BIT': 'INTEGER',
        'JSON': 'STRING',
        'NEWDECIMAL': 'INTEGER',
        'ENUM': 'STRING',
        'SET': 'STRING',
        'TINY_BLOB': 'STRING',
        'MEDIUM_BLOB': 'STRING',
        'LONG_BLOB': 'STRING',
        'BLOB': 'STRING',
        'VAR_STRING': 'STRING',
        'STRING': 'STRING',
        'GEOMETRY': 'STRING'
    }

    ADAPTER_TO_MYSQL = {
        'INTEGER': 'INTEGER',
        'FLOAT': 'FLOAT',
        'STRING': 'TEXT'
    }

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
        schema = self.__get_table_schema()
        row_iter = self.__get_row_iter()
        return Table(schema, row_iter)

    def __get_table_schema(self):
        column_names = [column[0] for column in self.__cursor.description]
        column_types = [column[1] for column in self.__cursor.description]
        column_types = self.__map_to_adapter_datatypes(column_types)
        schema = zip(column_names, column_types)
        return list(schema)

    def __map_to_adapter_datatypes(self, datatypes):
        adapter_datatypes = [self.MYSQL_TO_ADAPTER[FieldType.get_info(d)] for d in datatypes]
        return adapter_datatypes

    def __get_row_iter(self, chunksize=10):
        while True:
            rows = self.__cursor.fetchmany(chunksize)
            if rows:
                for row in rows:
                    yield row
            else:
                break

    def create_table(self, table, table_adress):
        self.__create_empty_table(table.schema, table_adress)
        self.__insert_data_in_table(table.row_iter, table_adress)

    def __create_empty_table(self, table_schema, table_name):
        table_schema_mysql = self.__format_table_schema(table_schema)
        query = f'CREATE TABLE {table_name} ({table_schema_mysql})'
        self.__cursor.execute(query)

    def __format_table_schema(self, table_schema):
        column_names, column_types = zip(*table_schema)
        column_types = self.__map_to_mysql_datatypes(column_types)
        formated_schema_list = [f'{col_name} {col_type}'
                                for col_name, col_type in zip(column_names, column_types)]
        formated_schema_string = ", ".join(formated_schema_list)
        return formated_schema_string

    def __map_to_mysql_datatypes(self, datatypes):
        mysql_datatypes = [self.ADAPTER_TO_MYSQL[datatype] for datatype in datatypes]
        return mysql_datatypes

    def __insert_data_in_table(self, row_iter, table_name):
        query = f'INSERT INTO {table_name} VALUES '
        for row in row_iter:
            self.__cursor.execute(query + f'{row}')
        self.__connection.commit()

    def delete_table(self, table_adress):
        query = f'DROP TABLE {table_adress}'
        self.__cursor.execute(query)


class AdapterCsv(AdapterAbstract):
    ADAPTER_TO_PYTHON = {
        'INTEGER': int,
        'FLOAT': float,
        'STRING': str
    }

    def __init__(self):
        super().__init__()

    def create_table(self, table, file_name):
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(table.schema)
            for row in table.row_iter:
                writer.writerow(row)

    def get_result_table(self, file_name):
        schema = self.__get_table_schema(file_name)
        row_iter = self.__get_row_iter(file_name)
        return Table(schema, row_iter)

    def __get_table_schema(self, file_name):
        header = self.__get_table_header(file_name)
        schema = self.__get_schema_from_header(header)
        return schema

    def __get_table_header(self, file_name):
        with open(file_name, newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)
        return header

    def __get_schema_from_header(self, header):
        schema = []
        for column in header:
            column_schema = re.findall("'(.+?)'", column)
            schema.append(tuple(column_schema))
        return schema

    def __get_row_iter(self, file_name):
        with open(file_name, newline='') as csvfile:
            reader = csv.reader(csvfile)
            schema = self.__get_schema_from_header(next(reader))
            for row in reader:
                converted_row = self.__convert_row(row, schema)
                yield(converted_row)

    def __convert_row(self, row, schema):
        _, column_types = zip(*schema)
        converted_row = []
        for col_nr, item in enumerate(row):
            item = self.__convert_to_python_type(item, column_types[col_nr])
            converted_row.append(item)
        return tuple(converted_row)

    def __convert_to_python_type(self, item, schema_type):
        convert = defaultdict(str, self.ADAPTER_TO_PYTHON)
        return convert[schema_type](item)

    def delete_table(self, file_name):
        os.remove(file_name)
