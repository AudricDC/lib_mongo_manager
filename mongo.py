from pymongo import MongoClient
from configparser import ConfigParser
import os


class MongoDBa(object):
    def __init__(self, host, port, database, collection, auth_source):
        try:
            config = ConfigParser()
            file_path = os.path.realpath(__file__)
            conf_path = os.path.join(os.path.dirname(file_path), r'mongo.ini')
            config.read(conf_path)
        except:
            config = ConfigParser()
            project_path = os.getcwd()
            module_path = r'mongo.ini'
            config.read(os.path.join(project_path, module_path))

        self.host = host
        self.port = int(port)

        self.username = config['MONGO_AUTHENTIFICATION']['USER']
        self.pwd = config['MONGO_AUTHENTIFICATION']['PWD']
        self.auth_source = auth_source

        # self.data_base = config['DATA_BASE']['DB_NAME']

        # self.collection = config['COLLECTIONS'][collection]
        self.client = MongoClient(host=self.host, port=self.port,
                                  username=self.username,
                                  password=self.pwd,
                                  authSource=self.auth_source)

        self.db = self.client[database]

        self.collection = self.db[collection]
