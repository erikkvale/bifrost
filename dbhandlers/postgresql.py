from .interface import DbHandler
from collections import OrderedDict
from sqlalchemy import create_engine


class PostgresHandler(DbHandler):

    def __init__(self, conn_str):
        super().__init__(conn_str)

    @property
    def connection(self):
        """
        Returns the underlying raw DBAPI connection
        from the SQLAlchemy engine
        """
        engine = create_engine(self.conn_str)
        if self._check_connection(engine):
            return engine.raw_connection()

    @property
    def engine(self):
        engine = create_engine(self.conn_str)
        if self._check_connection(engine):
            return engine

    def read(self):
        pass

    def write(self):
        pass

    def _check_connection(self, engine):
        try:
            conn = engine.connect()
            conn.close()
        except Exception:
            raise
        return True