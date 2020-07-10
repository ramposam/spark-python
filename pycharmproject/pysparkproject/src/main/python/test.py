from common.session.Session import Session
from common.conf.Config import Config
import sys
# sys.argv[1]= config\properties.txt
# sys.argv[2] = dev
configPath = sys.argv[1]
section = sys.argv[2]

def readFile( spark, inputDir):
    sc = spark.sparkContext
    file = sc.textFile(inputDir)
    file_fMap = file.flatMap(lambda lines: lines.split(" "))
    result = file_fMap.countByValue()
    return result

def writeFile( spark, result,outputDir):
    spark.sparkContext.parallelize(list(result.items())).coalesce(1).saveAsTextFile(outputDir)

if __name__ == "__main__":
    print("configPath:",configPath)
    print("section:", section)
    spark = Session().getSparkSession()
    conf = Config(configPath, section)
    inputDir = conf.srcDir
    outputDir = conf.tgtDir
    res = readFile(spark, inputDir)
    writeFile(spark,res, outputDir)
