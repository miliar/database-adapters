from google.cloud import bigquery


class AdapterBigquery():
    def __init__(self, service_acc):
        self.__client = bigquery.Client.from_service_account_json(service_acc)

    def get_result_iter(self, query):
        query_job = self.__client.query(query)
        return query_job.result()
