"""
DataFrame -> CSV file -> SQL table
SQL table -> CSV file -> DataFrame
"""
import os
import time
from tempfile import NamedTemporaryFile
from psycopg2 import sql
import sqlalchemy
import pandas
import numpy


class PostgreSqlHandler:

    def __init__(self, **kwargs):

        self._conn_dict = {
            'dbname': '',
            'user': '',
            'password': '',
            'host': 'localhost',
            'port': 5432
        }
        for attr, value in kwargs.items():
            if attr in self._conn_dict:
                self._conn_dict[attr] = value

        self._dsn_conn_str = (
            "postgresql://"
            "{user}:"
            "{password}@"
            "{host}:"
            "{port}/"
            "{dbname}".format(**self._conn_dict))

        self.engine = sqlalchemy.create_engine(
            self._dsn_conn_str, use_batch_mode=True
        )
        if self._check_engine(self.engine):
            self._dbapi_conn = self.engine.raw_connection()

    def dataframe_to_sql(self, dataframe, sql_table_name,
                         **to_csv_kwargs):
        try:
            with NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
                temp_csv.close()
                dataframe.to_csv(temp_csv, **to_csv_kwargs)
                result = self._to_sql(
                    temp_csv.name,
                    sql_table_name,
                    self._dbapi_conn
                )
        except:
            raise
        finally:
            os.unlink(temp_csv.name)
        return result


    def _to_sql(self, csv_file, table,
                conn):
        cursor = conn.cursor()
        table_composable = sql.Identifier(table)
        sql_query = (sql.SQL("COPY {} FROM STDIN WITH CSV HEADER;").
                     format(table_composable))
        with open(csv_file, mode='r') as f:
            cursor.copy_expert(sql=sql_query, file=f)
            conn.commit()
        return (
            cursor.rowcount,
            cursor.statusmessage
        )

    def _check_engine(self, engine):
        try:
            conn = engine.connect()
            conn.close()
        except:
            raise
        return True

if __name__ == '__main__':
    pg_handler = PostgreSqlHandler(
        dbname='test_pg',
        user='postgres',
        password='Gunnar14',
        host='localhost',
    )
    start_df = time.time()
    df = pandas.DataFrame(
        data=numpy.random.randint(
            0, 100,
            size=(20536725, 2)),
        columns=['A', 'B', 'C', 'D']
    )
    end_df = time.time()
    print("Df ")

    print('-'*20)

    start = time.time()
    result = pg_handler.dataframe_to_sql(df, 'test_data')
    end = time.time()
    elapsed_time = end - start
    print("psycopg2 copy_expert() time: {} Rows: {}".format(elapsed_time, result))

