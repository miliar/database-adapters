import os
import pickle
from adapters import Table


QUERY_RESULT_SCHEMA = [
    ('test1', 'STRING'),
    ('test2', 'INTEGER'),
    ('test3', 'FLOAT')
]
QUERY_RESULT = [
    ('value1_row1', 1, 1.1),
    ('value1_row2', 2, 2.2)
]
RESULT_TABLE = Table(QUERY_RESULT_SCHEMA, QUERY_RESULT)
TABLE_NAME_W = 'test_writing'
TABLE_NAME_R = 'test_table'


def get_query(table_ref):
    query = f"""
            SELECT *
            FROM `{table_ref}`
            ORDER BY 2
            """
    return query


# BIG_QUERY
SERVICE_ACC = os.path.abspath("bigquery_keys/bigquery_testkey.json")
DATASET = 'test_data'

# MYSQL


def get_config(file):
    with open(os.path.abspath(file), 'rb') as f:
        return pickle.load(f)


DB_CONFIG = get_config("mysql_keys/test_config.pickle")
"""
DB_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database'
}
"""
# CSV
SAVE_PATH = os.path.abspath("save_data")
CSV_TEMP_PATH = SAVE_PATH + "/temp.csv"
CSV_PATH = SAVE_PATH + "/test.csv"
