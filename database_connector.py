import mysql.connector
from mysql_config import db_config


class DatabaseConnector():
    def __init__(self, db_config):
        self.db_config = db_config

    def __enter__(self):
        self.connection = mysql.connector.connect(**self.db_config)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if traceback:
            print(f'Error : {exception_type} {exception_value}')  # todo : log
        self.cursor.close()
        self.connection.close()

    def get_result_iter(self, query, chunksize=10):
        self.cursor.execute(query)
        while True:
            rows = self.cursor.fetchmany(chunksize)
            if rows:
                yield rows
            else:
                break


with DatabaseConnector(db_config) as dbc:
    query = 'select * from watchlists limit 20'
    result = dbc.get_result_iter(query, 1)
    for rows in result:
        print(rows)
