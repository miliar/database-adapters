from google.cloud import bigquery
import logging
import traceback as tb


class AdapterBigquery():
    def __init__(self, service_acc):
        self.__client = bigquery.Client.from_service_account_json(service_acc)
        self.__logger = logging.getLogger(__name__)

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.__handle_exception(*exception)

    def __handle_exception(self, exc_type, exc_value, traceback):
        if traceback:
            exception = tb.format_exception(exc_type, exc_value, traceback)
            self.__logger.error(exception)

    def get_result_iter(self, query):
        query_job = self.__client.query(query)
        result = query_job.result()
        for row in result:
            yield row.values()
