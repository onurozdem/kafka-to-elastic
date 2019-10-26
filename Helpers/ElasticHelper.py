import Config as cfg
from elasticsearch import Elasticsearch, ElasticsearchException


class IndexHelper:
    def __init__(self):
        self.elasticsearch_client = None
        self.connection_ip = cfg.elasticsearch_host
        self.connection_port = cfg.elasticsearch_port
        self.connection_timeout = cfg.elasticsearch_timeout

        self.get_client()

    def get_client(self):
        try:
            self.elasticsearch_client = Elasticsearch([{'host': self.connection_ip, 'port': self.connection_port}], timeout=int(self.connection_timeout))
            if not self.elasticsearch_client.ping():
                raise Exception("Failed to connect to {} port {}: Connection refused.".format(self.connection_ip, self.connection_port))
            print("Elastic connection OK!")
        except ElasticsearchException as ese:
            raise Exception("Elasticsearch connection error; message: {} , args: {}".format(str(ese), str(ese.args)))

    def data_insert_index(self, index_name, data, doc_type_name="doc_type"):
        try:
            self.elasticsearch_client.index(index=index_name, doc_type=doc_type_name, body=data)
            print("Data inserted to Elasticsearch")
        except ElasticsearchException as ese:
            raise Exception("Elasticsearch data insert error; message: {}".format(str(ese)), 200, 1022)
        except Exception as e:
            raise e