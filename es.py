from elasticsearch import Elasticsearch
import requests


class Elastic_search:

    # Checking if Elasticsearch Server is Running
    def check_connection(self):
        return requests.get('http://localhost:9200')

    # Establishing Connection With Elasticsearch Server
    def establish_connection(self):
        return Elasticsearch([{'host': 'localhost', 'port': 9200}])

    # Writing Data into Elasticsearch from Spark DataFrame
    def write_data_onto_elasticsearch(self, df, name='name'):
        return df.write.format(
                "org.elasticsearch.spark.sql"
            ).option(
                "es.resource", '%s' % (name)
            ).option(
                "es.nodes", 'localhost'
            ).option(
                "es.port", '9200'
            )
