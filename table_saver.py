import csv
from adapters_testconfig import SAVE_PATH


class TableSaver():
    def __init__(self, query, adapter):
        self.__query = query
        self.__adapter = adapter

    def result_to_csv(self, file_name):
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            result = self.__adapter.get_result_iter(self.__query)
            for row in result:
                writer.writerow(row)

    def save_result_in(self, adapter, dataset_id, table_id):
        # csv = SAVE_PATH + '/temp.csv'
        # self.result_to_csv(csv)
        # adapter.create_table_from_csv(csv, dataset_id, table_id)
        row_iter = self.__adapter.get_result_iter(self.__query)
        adapter.create_table_from_iter(row_iter, dataset_id, table_id)
