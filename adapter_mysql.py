import mysql.connector
import logging
import traceback as tb


class AdapterMysql():
    def __init__(self, db_config):
        self.__db_config = db_config
        self.__logger = logging.getLogger(__name__)

    def __enter__(self):
        self.__connection = mysql.connector.connect(**self.__db_config)
        self.__cursor = self.__connection.cursor()
        return self

    def __exit__(self, *exception):
        self.__handle_exception(*exception)
        self.__cursor.close()
        self.__connection.close()

    def __handle_exception(self, exc_type, exc_value, traceback):
        if traceback:
            exception = tb.format_exception(exc_type, exc_value, traceback)
            self.__logger.error(exception)

    def get_result_iter(self, query, chunksize=10):
        self.__cursor.execute(query)
        while True:
            rows = self.__cursor.fetchmany(chunksize)
            if rows:
                for row in rows:
                    yield row
            else:
                break
