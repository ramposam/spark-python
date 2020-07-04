from com.conf.Config import Config
from com.session.Session import Session
import sys
import os
# sys.argv[1] = C:\Users\accountname\PycharmProjects\pysparkproject\src\main\python\config\properties.txt
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
