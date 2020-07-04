import configparser as cp

class Config:
    def __init__(self,configPath,section):
        props = cp.RawConfigParser()
        props.read(configPath)
        self.srcDir = props.get(section,"input.base.dir")
        self.tgtDir = props.get(section,"output.base.dir")

    def getInputDir(self):
        return self.srcDir

    def getOutputDir(self):
        return self.tgtDir
