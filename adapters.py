from abc import ABC, abstractmethod
from google.cloud import bigquery
import mysql.connector
import logging
import traceback as tb


class AdapterAbstract(ABC):
    def __init__(self):
        super().__init__()
        self.__logger = self.__get_logger()

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.__handle_exception(*exception)

    def __get_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.ERROR)
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger

    def __handle_exception(self, exc_type, exc_value, traceback):
        if traceback:
            exception = tb.format_exception(exc_type, exc_value, traceback)
            self.__logger.error(exception)

    @abstractmethod
    def get_result_iter(self, query, *args):
        pass


class AdapterBigquery(AdapterAbstract):
    def __init__(self, service_acc):
        super().__init__()
        self.__client = bigquery.Client.from_service_account_json(service_acc)

    def get_result_iter(self, query):
        query_job = self.__client.query(query)
        result = query_job.result()
        for row in result:
            yield row.values()

    def create_table_from_csv(self, csv, dataset_id, table_id):
        dataset_ref = self.__client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        with open(csv, 'rb') as source_file:
            job = self.__client.load_table_from_file(
                source_file,
                table_ref,
                location='US',
                job_config=job_config)

        job.result()

    def create_table_from_iter(self, row_iter, dataset_id, table_id):
        dataset_ref = self.__client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        schema = [bigquery.SchemaField("column1", "STRING", mode="NULLABLE"),
                  bigquery.SchemaField("column2", "STRING", mode="NULLABLE")]
        table = bigquery.Table(table_ref, schema=schema)
        table = self.__client.create_table(table)
        table = self.__client.get_table(table_ref)

        errors = self.__client.insert_rows(table, list(row_iter))
        print(errors)


class AdapterMysql(AdapterAbstract):
    def __init__(self, db_config):
        super().__init__()
        self.__db_config = db_config

    def __enter__(self):
        self.__connection = mysql.connector.connect(**self.__db_config)
        self.__cursor = self.__connection.cursor()
        return self

    def __exit__(self, *exception):
        super().__exit__(*exception)
        self.__cursor.close()
        self.__connection.close()

    def get_result_iter(self, query, chunksize=10):
        self.__cursor.execute(query)
        while True:
            rows = self.__cursor.fetchmany(chunksize)
            if rows:
                for row in rows:
                    yield row
            else:
                break
