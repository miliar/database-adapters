import csv


class QueryResultSaver():
    def __init__(self, query, adapter):
        self.__query = query
        self.__adapter = adapter

    def result_to_csv(self, file_name):
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            result = self.__adapter.get_result_iter(self.__query)
            for row in result:
                writer.writerow(row)
