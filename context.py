import findspark
findspark.init("/usr/local/spark/")
import os
os.environ["PYSPARK_PYTHON"] = '/usr/bin/python3'
from pyspark.sql import SparkSession


class Context:

    # function for creating SparkContext
    def context(self):
        try:
            spark = SparkSession.builder.getOrCreate()
        except:
            print("Session doesn't get Created")
        return spark

    # Load Data from Csv File
    def load_data(self, spark):
        try:
            df = spark.read.csv("/home/dewanshu/Downloads/HR2m.csv", header=True, inferSchema=True)
        except:
            print("Data didn't get loaded")
        return df
