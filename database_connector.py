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
            print(f'Error : {exception_type} {exception_value}') # implement logging
        self.cursor.close()
        self.connection.close()

    def print_query(self, query):
        self.cursor.execute(query)
        print(self.cursor.fetchall())


with DatabaseConnector(db_config) as dbc:
    query = ('select * from watchlists limit 2')
    dbc.print_query(query)
