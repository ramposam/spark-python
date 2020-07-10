from src.main.python.common.session.Session import Session
from src.main.python.common.conf.Config import  Config
import sys
import os
# Need to change working directory to python folder
# sys.argv[1] = config\properties.txt
# sys.argv[2]= dev

class WordCount:
    configPath = os.path.abspath(sys.argv[1])
    section = sys.argv[2]

    def readFile(spark, inputDir):
        sc = spark.sparkContext
        file = sc.textFile(inputDir)
        file_fMap = file.flatMap(lambda lines: lines.split(" "))
        words = file_fMap.map(lambda words: (words, 1))
        result = words.reduceByKey(lambda tot, ele: tot + ele)
        return  result

    def writeFile(spark, result,outputDir):
        result.coalesce(1).saveAsTextFile(outputDir)

    if __name__ == "__main__":
        print("configpath:",configPath)
        print("section:",section)
        spark = Session().getSparkSession()
        conf = Config(configPath, section)
        inputDir = conf.getInputDir()
        outputDir = conf.getOutputDir()
        res = readFile(spark, inputDir)
        writeFile(spark, res ,outputDir)
