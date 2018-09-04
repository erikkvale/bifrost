from .interface import DbHandler
from collections import OrderedDict
from sqlalchemy import create_engine
from psycopg2 import sql
import pandas
import io


class SqlDbHandler:

    def __init__(self, conn_str):
        self.conn_str = conn_str

    @property
    def engine(self):
        """Returns the SQLAlchemy engine"""
        return create_engine(self.conn_str)

    @property
    def connection(self):
        """Returns the underlying DBAPI raw connection"""
        return self.engine.raw_connection()

    def read_sql(self, *args, **kwargs):
        """Reads SQL data into Pandas DataFrame"""
        return pandas.read_sql(*args, **kwargs)

    def to_sql(self, dataframe, table,
               bulk_load=True, *args, **kwargs):
        """Loads a Pandas DataFrame to SQL with optional bulk insert"""
        str_buff = io.StringIO()
        dataframe.to_csv(str_buff)
        table_composable = sql.Identifier(table)
        sql_query = (sql.SQL("COPY {} FROM STDIN WITH CSV HEADER;").
                     format(table_composable))
        cursor = self.connection.cursor()
        cursor.copy_expert(
            file=str_buff.seek(0),
            sql=sql_query
        )
        self.connection.commit()

    def __str__(self):
        pass

    def __repr__(self):
        pass
