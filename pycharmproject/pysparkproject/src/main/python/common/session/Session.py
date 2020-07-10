from pyspark.sql import SparkSession

class Session:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Spark Session").master("local[*]").getOrCreate()

    def __init__(self,**jar):
        if jar.__len__() == 0:
            self.spark = SparkSession.builder.appName("Spark Session").master("local[*]").getOrCreate()
        else:
            self.spark = SparkSession.\
                builder.\
                appName("Mysql Spark Session").\
                config("spark.jars.packages",jar["name"]). \
                config("spark.jars",jar["path"]).\
                master("local[*]").\
                getOrCreate()

    def getSparkSession(self):
        print("Session created..")
        return self.spark
