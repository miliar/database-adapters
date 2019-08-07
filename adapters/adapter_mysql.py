from adapters.adapter_abstract import AdapterAbstract, Table
import mysql.connector
from mysql.connector import FieldType


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
        adapter_datatypes = [
            self.MYSQL_TO_ADAPTER[FieldType.get_info(d)] for d in datatypes]
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
        mysql_datatypes = [self.ADAPTER_TO_MYSQL[datatype]
                           for datatype in datatypes]
        return mysql_datatypes

    def __insert_data_in_table(self, row_iter, table_name):
        query = f'INSERT INTO {table_name} VALUES '
        for row in row_iter:
            self.__cursor.execute(query + f'{row}')
        self.__connection.commit()

    def delete_table(self, table_adress):
        query = f'DROP TABLE {table_adress}'
        self.__cursor.execute(query)
