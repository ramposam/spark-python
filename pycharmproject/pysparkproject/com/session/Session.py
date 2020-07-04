from pyspark.sql import SparkSession


class Session:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Spark Session").master("local[*]").getOrCreate()

    def getSparkSession(self):
        return self.spark
