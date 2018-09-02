from .interface import DbHandler
from collections import OrderedDict
from sqlalchemy import create_engine


class PostgresHandler(DbHandler):

    def __init__(self, conn_str):
        super().__init__(conn_str)

    def connection(self):
        pass

    def read(self):
        pass

    def write(self):
        pass