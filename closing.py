class Closing:

    def spark_closing(self, sc):
        return sc.stop()


    def elastic_closing(self, es):
        return es.transport.close()
