import configparser as cp

class Config:
    def __init__(self,configPath,section):
        props = cp.RawConfigParser()
        props.read(configPath)
        if section == "dev":
            self.srcDir = props.get(section,"input.base.dir")
            self.tgtDir = props.get(section,"output.base.dir")
        elif section == "mysql":
            self.hostname = props.get(section, "hostname")
            self.username = props.get(section, "username")
            self.password = props.get(section, "password")
            self.database = props.get(section, "database")
            self.driver = props.get(section, "driver")
            self.libarayPath = props.get(section, "libpath")
            self.table = props.get(section, "table")
            self.outdir = props.get(section, "output.dir")