from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
import configparser as cp
import sys
import os
from datetime import datetime
#os.environ["SPARK_HOME"]=r"C:\Users\rposam\Downloads\spark-2.4.5-bin-hadoop2.7\spark-2.4.5-bin-hadoop2.7"
#os.environ["HADOOP_HOME"]=r"C:\Users\rposam\Downloads\spark-2.4.5-bin-hadoop2.7\spark-2.4.5-bin-hadoop2.7"
#os.environ["PYSPARK_PYTHON"]=r"C:\Users\rposam\AppData\Local\Programs\Python\Python37\python.exe"
import time
props = cp.RawConfigParser()
props.read("config/properties.txt")
section = sys.argv[1]
mode = props.get(section,"executionmode")
spark = SparkSession.builder \
     .master(mode) \
     .appName("Pyspark Word count program") \
     .getOrCreate()
sc = spark.sparkContext
inputdir = props.get(section,"input.base.dir")
outputdir = props.get(section,"output.base.dir")
for conf in spark.sparkContext.getConf().getAll():
     print(conf)
inputpath = inputdir+"/file.txt"
print("Input file path: {} ".format(inputpath))
print("Reading file started at {}".format(datetime.now()))
t = time.time()
f = sc.textFile(inputpath)
result = f.flatMap(lambda line: line.split(" ")).countByValue()
#RDD's doesn't have overwrite option(ONly DF has)
outputpath = outputdir+"/word_cnt"
print("output file path:{}".format(outputpath))
sc.parallelize(list(result.items())).coalesce(1).saveAsTextFile("output/word_cnt")
print("File Read and write completed at {} - Time taken to complete(inSecs) {}".format(datetime.now(),time.time()-t))
spark.stop()