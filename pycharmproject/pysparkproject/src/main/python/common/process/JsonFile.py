import json
from src.main.python.common.session.Session import Session


class JsonFile:

    def read(self,spark,input):
        df = spark.read.json(input)
        return df
