import csv
from adapters_testconfig import SAVE_PATH


class TableSaver():
    def __init__(self, query, adapter):
        self.__query = query
        self.__adapter = adapter

    def result_to_csv(self, file_name):
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            schema, result = self.__adapter.get_result_table(self.__query)  # rewrite for table schema
            for row in result:
                writer.writerow(row)

    def save_result_in(self, adapter, dataset_id, table_id):
        pass
