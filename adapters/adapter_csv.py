from adapter_abstract import AdapterAbstract, Table
from collections import defaultdict
import csv
import re
import os


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
