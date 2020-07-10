from src.main.python.common.conf.Config import Config
from src.main.python.common.session.Session import Session
import sys
class MysqlRead:
    configpath = sys.argv[1]
    section = sys.argv[2]
    conf = Config(configpath,section)
    username = conf.username
    password = conf.password
    database = conf.database
    hostname = conf.hostname
    driver = conf.driver
    jar = "mysql:mysql-connector-java:5.1.44"
    librarypath = conf.libarayPath
    table = conf.table
    outputDir = conf.outdir
    url = hostname+"/"+database
    #spark.read.format("jdbc")

    spark = Session(name=jar, path=librarypath).getSparkSession()
    tableRead = spark.\
        read.\
        format("jdbc").\
        option("url",url). \
        option("driver",driver). \
        option("user",username). \
        option("password",password). \
        option("dbtable",table). \
        load()
    tableRead.write.json(outputDir)