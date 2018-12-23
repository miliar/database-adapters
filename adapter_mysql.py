import mysql.connector


class AdapterMysql():
    def __init__(self, db_config):
        self.db_config = db_config

    def __enter__(self):
        self.connection = mysql.connector.connect(**self.db_config)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if traceback:
            pass  # todo : log
        self.cursor.close()
        self.connection.close()

    def get_result_iter(self, query, chunksize=10):
        self.cursor.execute(query)
        while True:
            rows = self.cursor.fetchmany(chunksize)
            if rows:
                for row in rows:
                    yield row
            else:
                break
